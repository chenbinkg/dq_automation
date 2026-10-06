pipeline {
    agent any

    options {
        disableConcurrentBuilds()
        timestamps()
        timeout(time: 1, unit: 'DAYS')
    }

    parameters {
        booleanParam(
            name: 'RUN_AFTER_DEPLOY',
            defaultValue: false,
            description: 'Run dq_automation_pipeline after a successful dev/main deployment.'
        )
    }

    environment {
        CI_VENV = "${WORKSPACE}/.venv-ci"
        DATABRICKS_AUTH_TYPE = 'oauth-m2m'
    }

    stages {
        stage('Select target') {
            steps {
                script {
                    if (env.BRANCH_NAME == 'main') {
                        env.DEPLOY_TARGET = 'prod'
                        env.DATABRICKS_HOST = 'https://dbc-1d2e2f8a-2140.cloud.databricks.com'
                        env.DATABRICKS_CLUSTER_ID = '0128-064509-tf6od0jl'
                    } else {
                        env.DEPLOY_TARGET = 'dev'
                        env.DATABRICKS_HOST = 'https://dbc-1a2fed98-ca15.cloud.databricks.com'
                        env.DATABRICKS_CLUSTER_ID = '1023-021822-wqfpujup'
                    }
                    env.DATABRICKS_CREDENTIAL_ID = "dq-automation-databricks-${env.DEPLOY_TARGET}"
                }
            }
        }

        stage('Python checks') {
            steps {
                sh '''
                    set -eu
                    rm -rf "$CI_VENV"
                    python3 -m venv "$CI_VENV"
                    "$CI_VENV/bin/python" -m pip install --disable-pip-version-check -r requirements-ci.txt
                    "$CI_VENV/bin/python" -m py_compile \
                        pipeline_io.py \
                        postgres_io.py \
                        s1_query_dataset_and_bu.py \
                        s2_query_dataset_details.py \
                        s3_prepare_jjdmc_report.py \
                        s5_generate_potential_jira_tickets.py \
                        s6_log_new_jira_tickets.py \
                        s7_invalidate_jira_tickets.py \
                        s8_diagnose_jira_issues.py \
                        s9_send_email_notification.py
                    "$CI_VENV/bin/python" -m unittest -v test_pipeline_postgres_io.py
                '''
            }
        }

        stage('Databricks preflight') {
            when {
                anyOf {
                    branch 'dev'
                    branch 'main'
                }
            }
            steps {
                withCredentials([
                    usernamePassword(
                        credentialsId: env.DATABRICKS_CREDENTIAL_ID,
                        usernameVariable: 'DATABRICKS_CLIENT_ID',
                        passwordVariable: 'DATABRICKS_CLIENT_SECRET'
                    )
                ]) {
                    sh '''
                        set -eu
                        databricks version
                        databricks current-user me >/dev/null
                        databricks secrets list-secrets collibra >/dev/null
                        databricks clusters get "$DATABRICKS_CLUSTER_ID" >/dev/null
                        databricks bundle validate -t "$DEPLOY_TARGET"
                    '''
                }
            }
        }

        stage('Approve production') {
            when {
                branch 'main'
            }
            input {
                message 'Deploy the validated bundle to production?'
                ok 'Deploy to production'
                submitterParameter 'DEPLOY_APPROVER'
            }
            steps {
                echo "Production deployment approved by ${env.DEPLOY_APPROVER}"
            }
        }

        stage('Deploy') {
            when {
                anyOf {
                    branch 'dev'
                    branch 'main'
                }
            }
            steps {
                withCredentials([
                    usernamePassword(
                        credentialsId: env.DATABRICKS_CREDENTIAL_ID,
                        usernameVariable: 'DATABRICKS_CLIENT_ID',
                        passwordVariable: 'DATABRICKS_CLIENT_SECRET'
                    )
                ]) {
                    sh '''
                        set -eu
                        databricks bundle deploy \
                            -t "$DEPLOY_TARGET" \
                            --auto-approve \
                            --fail-on-active-runs
                    '''
                }
            }
        }

        stage('Run pipeline') {
            when {
                allOf {
                    anyOf {
                        branch 'dev'
                        branch 'main'
                    }
                    expression {
                        return params.RUN_AFTER_DEPLOY
                    }
                }
            }
            steps {
                withCredentials([
                    usernamePassword(
                        credentialsId: env.DATABRICKS_CREDENTIAL_ID,
                        usernameVariable: 'DATABRICKS_CLIENT_ID',
                        passwordVariable: 'DATABRICKS_CLIENT_SECRET'
                    )
                ]) {
                    sh '''
                        set -eu
                        databricks bundle run dq_automation_pipeline -t "$DEPLOY_TARGET"
                    '''
                }
            }
        }
    }

    post {
        always {
            sh 'rm -rf "$CI_VENV"'
        }
    }
}
