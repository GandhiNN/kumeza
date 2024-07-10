// -----------------------------------------------------------------------------
// Variable Settings
// -----------------------------------------------------------------------------
ZIP_NAME = 'kumeza'
AWS_REGION = 'eu-west-1'
BB_PROJECT_NAME = 'daa1'
STREAM = 'daas'

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
SERVICE_ACCOUNT = "${BB_PROJECT_NAME}-${STAGE}-aws"

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

def setup() {
    sh(script: """
    yum install -y zip jq python3.8-dev python3.8-venv
    curl -sSL https://install.python-poetry.org | POETRY_HOME=/etc/poetry python3 -
    /etc/poetry/bin/poetry --version
    """)
}

def unitTest() {
    sh(script: "make test")
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
                    - name: sam-build-python
                      image: art.pmideep.com:8080/dockerhub-cache/amazon/aws-sam-cli-build-image-python3.8:latest
                      resources:
                        requests:
                            cpu: 1
                            memory: 1Gi
                        limits:
                            cpu: 2
                            memory: 2Gi
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
    environment {
        AWS_ACCOUNT_0 = "${awsAccount0}"
        AWS_ACCOUNT_1 = "${awsAccount1}"
        AWS_ACCOUNT_ID = "${awsAccountId}"
    }
    stages {
        stage("Prepare and check") {
            steps {
                container("awscli") {
                    sh '''
                        SESSION_NAME=$(echo ${BRANCH_NAME}-${BUILD_NUMBER} | sed 's,/,-,g')
                        printf "AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\nAWS_SESSION_TOKEN=%s" \
                        $(aws sts assume-role-with-web-identity \
                        --role-arn arn:aws:iam::${AWS_ACCOUNT_ID}:role/DAAS-Jenkins \
                        --role-session-name ${SESSION_NAME} \
                        --web-identity-token file://${AWS_WEB_IDENTITY_TOKEN_FILE} \
                        --duration-seconds 3600 \
                        --query "Credentials.[AccessKeyId,SecretAccessKey,SessionToken]" \
                        --output text) | tee credential.txt \
                        cat credential.txt
                    '''
                }
            }
        }
        stage('Poetry Configuration') {
            steps {
                container("sam-build-python") {
                    script {
                        setup()
                    }
                }
            }
        }
    }
}