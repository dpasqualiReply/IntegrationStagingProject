pipeline {
    agent any
    parameters {
        string(name: 'APP_TO_INTEGRATE', defaultValue: '???', description: 'The newly updated assembly that you want to integration test')
    }
    stages {
        stage('Retrieve Assembly from deploy dir') {
            steps {
                sh 'sudo cp /opt/deploy/batchETL/*-assembly-*.jar lib/'
                sh 'sudo cp /opt/deploy/mrSpark2/*-assembly-*.jar lib/'
                sh 'sudo cp /opt/deploy/realTimeETL/*-assembly-*.jar lib/'
                sh 'sudo cp /opt/deploy/realTimeMovieRec/*-assembly-*.jar lib/'
            }
        }
        stage('Integration Tests') {
            steps {
                sh 'sbt clean test'
                archiveArtifacts 'target/test-reports/*.xml'
            }
        }
        stage('Production Deploy') {
            steps {
                echo 'Safe to Deploy ${params.APP_TO_INTEGRATE} in production'
                echo ' You make a great job :D'
            }
        }
    }
}