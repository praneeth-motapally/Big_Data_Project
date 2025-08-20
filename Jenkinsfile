pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Setup Environment') {
            steps {
                echo "Installing dependencies"
                sh 'pip install -r requirements.txt'
            }
        }

        stage('Run Tests') {
            steps {
                echo "Running tests for transformations"
                script {
                    if (env.BRANCH_NAME == 'feature1-data-extraction-and-table-creation') {
                        sh 'pytest tests/test_transform_data1.py'
                    } else if (env.BRANCH_NAME == 'feature2-3-tables-transformation') {
                        sh 'pytest tests/test_transform_data2.py'
                    } else if (env.BRANCH_NAME == 'feature3-another-3-tables-transformation') {
                        sh 'pytest tests/test_transform_data3.py'
                    } else if (env.BRANCH_NAME == 'feature4-some-few-transformation') {
                        sh 'pytest tests/test_transform_data4.py'
                    } else if (env.BRANCH_NAME == 'feature5-LLM-usage') {
                        echo "No dedicated transform tests for feature5, skipping transform tests"
                        // Optionally run general tests here if needed:
                        // sh 'pytest tests/'
                    } else {
                        echo "Unknown branch, running general tests"
                        sh 'pytest tests/'
                    }
                }
            }
        }

        stage('Run Pipeline') {
            steps {
                script {
                    if (env.BRANCH_NAME == 'feature1-data-extraction-and-table-creation') {
                        sh 'python pipelines/pipeline1.py'
                    } else if (env.BRANCH_NAME == 'feature2-3-tables-transformation') {
                        sh 'python pipelines/pipeline2.py'
                    } else if (env.BRANCH_NAME == 'feature3-another-3-tables-transformation') {
                        sh 'python pipelines/pipeline3.py'
                    } else if (env.BRANCH_NAME == 'feature4-some-few-transformation') {
                        sh 'python pipelines/pipeline4.py'
                    } else if (env.BRANCH_NAME == 'feature5-LLM-usage') {
                        sh 'python pipelines/pipeline5.py'
                    } else {
                        error "Unknown branch: ${env.BRANCH_NAME}"
                    }
                }
            }
        }
    }

    post {
        always {
            echo 'Cleaning up workspace'
            deleteDir()
        }
        success {
            echo 'Build succeeded!'
        }
        failure {
            echo 'Build failed!'
        }
    }
}
