def awsAccount0
def awsAccount1
def awsAccountId
def libVersion

if (BRANCH_NAME == "master") {
    awsAccount0 = "prd"
    awsAccount1 = "prod"
    awsAccountId = "568650317375"
}  else if (BRANCH_NAME.startsWith("release/") || (BRANCH_NAME == "qa")) {
    awsAccount0 = "qa"
    awsAccount1 = "qa"
    awsAccountId = "326845138312"
}   else if (BRANCH_NAME.startsWith("feature/") || (BRANCH_NAME == "dev")) {
    awsAccount0 = "dev"
    awsAccount1 = "dev"
    awsAccountId = "291751643970"
} else {
    error("This branch is not allowed")
    currentBuild.result = "ABORTED"
}

pipeline {
    agent {
        kubernetes {
            yaml """
                apiVersion: v1
                kind: Pod
                spec:
                    serviceAccountName: 'icloud-${awsAccount0}-aws'
                    imagePullSecrets: [ 'artifactory-icloud-dev' ]
                    containers:
                    - name: awscli
                      image: art.pmideep.com/dockerhub/amazon/aws-cli:2.17.4
                      command:
                      - cat 
                      tty: true
                    - name: python
                      image: art.pmideep.com/dockerhub/python:3.9.10
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
    }
}