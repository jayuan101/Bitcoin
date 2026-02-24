pipeline {
    agent any

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
                  
                  # Switch to root and install Python (Ubuntu/Debian)
                  sudo apt-get update
                  sudo apt-get install -y python3 python3-pip python3-venv
                    
                  # Create venv using python3
                  python3 -m venv .venv
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
                  . ${PYTHON_ENV}/bin/activate
                  python bitcoin_etl.py
                '''
            }
        }

        stage('Archive Parquet') {
            steps {
                archiveArtifacts artifacts: 'data/**/*.parquet', allowEmptyArchive: true
            }
        }
    }

    post {
        success {
            echo "✅ Pipeline completed successfully"
        }
        failure {
            echo "❌ Pipeline failed - check logs"
        }
    }
}