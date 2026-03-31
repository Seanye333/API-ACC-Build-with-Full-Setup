pipeline {
    agent any

    environment {
        DOCKER_IMAGE = 'acc-build-etl'
        DOCKER_TAG   = "${env.BUILD_NUMBER}"
        REGISTRY     = credentials('docker-registry-url')  // configure in Jenkins
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Lint') {
            steps {
                sh 'pip install flake8'
                sh 'flake8 etl/ dags/ config/ --max-line-length=100 --ignore=E501'
            }
        }

        stage('Test') {
            steps {
                sh 'pip install -r requirements.txt'
                sh 'pip install pytest'
                sh 'pytest tests/ -v --tb=short'
            }
        }

        stage('Docker Build') {
            steps {
                dir('docker') {
                    sh "docker build -t ${DOCKER_IMAGE}:${DOCKER_TAG} -f Dockerfile .."
                }
            }
        }

        stage('Docker Push') {
            steps {
                sh "docker tag ${DOCKER_IMAGE}:${DOCKER_TAG} ${REGISTRY}/${DOCKER_IMAGE}:${DOCKER_TAG}"
                sh "docker tag ${DOCKER_IMAGE}:${DOCKER_TAG} ${REGISTRY}/${DOCKER_IMAGE}:latest"
                sh "docker push ${REGISTRY}/${DOCKER_IMAGE}:${DOCKER_TAG}"
                sh "docker push ${REGISTRY}/${DOCKER_IMAGE}:latest"
            }
        }

        stage('Deploy') {
            steps {
                dir('docker') {
                    sh 'docker-compose down || true'
                    sh 'docker-compose up -d'
                }
            }
        }

        stage('Trigger DAG') {
            steps {
                sh '''
                    curl -X POST "http://localhost:8080/api/v1/dags/acc_build_etl/dagRuns" \
                      -H "Content-Type: application/json" \
                      -u admin:admin \
                      -d '{"conf": {}}'
                '''
            }
        }
    }

    post {
        failure {
            echo 'Pipeline failed — check logs above.'
        }
        success {
            echo 'Pipeline completed successfully.'
        }
        always {
            cleanWs()
        }
    }
}
