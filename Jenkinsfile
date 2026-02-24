pipeline {
    agent {
        docker {
            image 'bitnami/spark:latest'
        }
    }
    environment {
        PYTHON_ENV = ".venv"
    }
    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }
        stage('Install') {
            steps {
                sh '''
                    set -e
                    python -m venv .venv
                    . .venv/bin/activate
                    pip install --upgrade pip
                    pip install -r requirements.txt
                '''
            }
        }
        stage('Run Bitcoin ETL') {
            steps {
                sh '''
                    set -e
                    . .venv/bin/activate
                    python bitcoin.py
                '''
            }
        }
        stage('Archive Parquet') {
            steps {
                archiveArtifacts artifacts: 'bronze/**,silver_outputs/**,gold/**', fingerprint: true
            }
        }
    }
    post {
        success {
            echo "Pipeline completed successfully"
        }
        failure {
            echo "Pipeline failed - check logs"
        }
    }
}
