//pipeline {
//  agent any
//  stages {
//    stage('Retrieve Assembly from deploy dir') {
//      steps {
//        sh 'sudo cp /opt/deploy/batchETL/BatchETL2-assembly-0.1.jar lib/'
//        sh 'sudo cp /opt/deploy/mrSpark2/MRSpark2-assembly-0.1.jar lib/'
//        sh 'sudo cp /opt/deploy/realTimeETL/RealTimeETL-assembly-0.1.jar lib/'
//        sh 'sudo cp /opt/deploy/realTimeMovieRec/RealTimeMovieRec-assembly-0.1.jar lib/'
//        sh 'ls lib/'
//      }
//    }
//    stage('Integration Tests') {
//      steps {
//        sh 'sbt clean test'
//        archiveArtifacts 'target/test-reports/*.xml'
//      }
//    }
//    stage('Production Deploy') {
//      steps {
//        echo 'Safe to Deploy ${params.APP_TO_INTEGRATE} in production'
//        echo ' You make a great job :D'
//      }
//    }
//  }
//  parameters {
//    string(name: 'APP_TO_INTEGRATE', defaultValue: '???', description: 'The newly updated assembly that you want to integration test')
//  }
//}