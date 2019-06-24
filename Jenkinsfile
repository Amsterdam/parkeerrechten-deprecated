#!groovy

def tryStep(String message, Closure block, Closure tearDown = null) {
    try {
        block();
    }
    catch (Throwable t) {
        slackSend message: "${env.JOB_NAME}: ${message} failure ${env.BUILD_URL}", channel: '#ci-channel', color: 'danger'

        throw t;
    }
    finally {
        if (tearDown) {
            tearDown();
        }
    }
}


node {
    stage("Checkout") {
        checkout scm
    }

    stage("Test") {
        tryStep "test", {
            sh "src/.jenkins/test/test.sh"
        }
    }

    stage("Build image") {
        tryStep "build", {
            def image = docker.build("repo.data.amsterdam.nl/datapunt/parkeerrechten:${env.BUILD_NUMBER}")
            image.push()
        }
    }
}

String BRANCH = "${env.BRANCH_NAME}"

if (BRANCH == "master") {

    node {
        stage('Push acceptance image') {
            tryStep "image tagging", {
                def image = docker.image("repo.data.amsterdam.nl/datapunt/parkeerrechten:${env.BUILD_NUMBER}")
                image.pull()
                image.push("acceptance")
            }
        }
    }


    stage('Waiting for approval') {
        slackSend channel: '#ci-channel', color: 'warning', message: 'parkeerrechten is waiting for Production Release - please confirm'
        input "Deploy to Production?"
    }

    node {
        stage('Push production image') {
            tryStep "image tagging", {
                def image = docker.image("repo.data.amsterdam.nl/datapunt/parkeerrechten:${env.BUILD_NUMBER}")
                image.pull()
                image.push("production")
                image.push("latest")
            }
        }
    }
}