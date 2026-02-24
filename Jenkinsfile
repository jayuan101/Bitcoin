pipeline {
  agent any

  environment {
    SPARK_HOME  = "C:\\spark"
    HADOOP_HOME = "C:\\hadoop"
    JAVA_HOME   = "C:\\Program Files\\Zulu\\zulu-21"
    TEMP        = "C:\\tmp\\spark"
    TMP         = "C:\\tmp\\spark"
  }

  stages {
    stage("Checkout") {
      steps { checkout scm }
    }
    stage("Install") {
      steps { bat "python -m pip install -r requirements.txt --quiet" }
    }
    stage("Run Bitcoin ETL") {
      steps { bat "python bitcoin.py" }
    }
    stage("Archive Parquet") {
      steps {
        archiveArtifacts artifacts: "bronze/**,silver_outputs/**,gold/**",
                         fingerprint: true
      }
    }
  }

  post {
    success { echo "Pipeline succeeded!" }
    failure  { echo "Pipeline failed - check logs" }
  }
}