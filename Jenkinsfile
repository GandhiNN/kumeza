// -----------------------------------------------------------------------------
// Variable Settings
// -----------------------------------------------------------------------------
ZIP_NAME = 'kumeza'
AWS_REGION = 'eu-west-1'
BB_PROJECT_NAME = 'daa1'
STREAM = 'daas'
DEEP_PRODUCT_NAME = 'icloud'

// Conditional switch for environment
if (env.BRANCH_NAME == "master") {
    DEEP_ENVIRONMENT = 'prd'
    ARTIFACTORY_ENV = 'prod'
    STAGE = 'prd'
}  else if (env.BRANCH_NAME.startsWith("release/") || (BRANCH_NAME == "qa")) {
    DEEP_ENVIRONMENT = 'qa'
    ARTIFACTORY_ENV = 'qa'
    STAGE = 'qa'
}   else if (BRANCH_NAME.startsWith("feature/") || (BRANCH_NAME == "dev")) {
    DEEP_ENVIRONMENT = 'dev'
    ARTIFACTORY_ENV = 'dev'
    STAGE = 'dev'
} else {
    error("This branch is not allowed")
    currentBuild.result = "ABORTED"
}

// Set S3 bucket name
S3_BUCKET_NAME = "${STREAM}-s3-library-${DEEP_ENVIRONMENT}"

// Set service account
SERVICE_ACCOUNT = "${DEEP_PRODUCT_NAME}-${STAGE}-aws"

// Information discovered on runtime
repoVersion = ''
lambdas = []

// -----------------------------------------------------------------------------
// Auxiliar functions
// -----------------------------------------------------------------------------

// Use AWS CLI to discover the current AWS Account ID
def discoverAccountID() {
    return sh(returnStdout: true, script: "aws sts get-caller-identity | jq -r \".Account\"").trim()
}

def initSetup() {
    sh(script: """
    apt update; apt install build-essential jq zip default-jdk -y --no-install-recommends
    make init
    """)
}

def poetrySetup() {
    sh(script: """
    make install-poetry
    make install
    """)
}

def formatAndLintCodebase() {
    sh(script: """
    make format
    make lint
    """)
}

def unitTest() {
    sh(script: "make test")
}

def buildWheel() {
    sh(script: "make build")
}

def copyWheelToS3Bucket() {
    sh(script: """
    echo 'Transferring wheel file to S3'
    aws s3 cp ./dist/ s3://${S3_BUCKET_NAME}/python/ --recursive --exclude \"*.gz\"
    """)
}

pipeline {
    agent {
        kubernetes {
            inheritFrom "${SERVICE_ACCOUNT}"
            yaml """
                apiVersion: v1
                kind: Pod
                spec:
                    serviceAccountName: '${SERVICE_ACCOUNT}'
                    imagePullSecrets: [ 'artifactory-icloud-dev' ]
                    containers:
                    - name: awscli
                      image: art.pmideep.com/dockerhub/amazon/aws-cli:2.17.4
                      command:
                      - cat 
                      tty: true
                    - name: python
                      image: art.pmideep.com/dockerhub/python:3.9.19-slim
                      command:
                      - cat
                      tty: true
                    - name: pyspark
                      image: art.pmideep.com/dockerhub/apache/spark-py:latest
                      command:
                      - cat
                      tty: true
                    - name: sonarscanner
                      image: art.pmideep.com/dockerhub/sonarsource/sonar-scanner-cli:5
                      command:
                      - cat
                      tty: true
                    - name: zip
                      image: art.pmideep.com/dockerhub/javieraviles/zip:latest
                      command:
                      - cat
                      tty: true
            """
        }
    }
    stages {
        stage('Run CI?') {
            agent any
            steps {
                script {
                    if (sh(script: "git log -1 --pretty=%B | fgrep -ie '[skip ci]' -e '[ci skip]'", returnStatus: true) == 0) {
                        currentBuild.result = 'NOT BUILT'
                        error 'Aborting because commit message contains [skip ci]'
                    }
                }
            }
        }
        stage('Initial Setup') {
            steps {
                container('pyspark') {
                    script {
                        echo "Using DEEP environment: ${DEEP_ENVIRONMENT}"
                        echo "Using Service Account: ${SERVICE_ACCOUNT}"
                        initSetup()
                        accountId = discoverAccountID()
                    }
                }
            }
        }
        stage('Install and Setup Poetry') {
            steps {
                container("pyspark") {
                    script {
                        poetrySetup()
                    }
                }
            }
        }
        stage('Format and Lint the Codebase') {
            steps {
                container("pyspark") {
                    script {
                        formatAndLintCodebase()
                    }
                }
            }
        }
        stage('Unit Test') {
            steps {
                container("pyspark") {
                    script {
                        unitTest()
                    }
                }
            }
        }
        stage('Build Wheel File') {
            steps {
                container("pyspark") {
                    script {
                        buildWheel()
                    }
                }
            }
        }
        stage('Sync Wheel File') {
            steps {
                container("pyspark") {
                    script {
                        copyWheelToS3Bucket()
                    }
                }
            }
        }
    }
}