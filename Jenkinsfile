def venvPath = 'D:\\Big_Data_Project\\.venv\\Scripts'

pipeline {
    agent any

    environment {
        GROQ_API_KEY = credentials('groq-api-key')   // Jenkins credential ID
        GROQ_MODEL   = 'llama-3.3-70b-versatile'     // You can change if needed
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Setup Environment') {
            steps {
                echo "Installing dependencies"
                bat "${venvPath}\\pip.exe install -r requirements.txt"
            }
        }

        stage('Run Tests') {
            steps {
                echo "Running tests"
                script {
                    dir(pwd()) {
                        if (env.BRANCH_NAME == 'feature1-data-extraction-and-table-creation') {
                            bat "${venvPath}\\pytest.exe tests\\test_transform_data1.py"
                        } else if (env.BRANCH_NAME == 'feature2-3-tables-transformation') {
                            bat "${venvPath}\\pytest.exe tests\\test_transform_data2.py"
                        } else if (env.BRANCH_NAME == 'feature3-another-3-tables-transformation') {
                            bat "${venvPath}\\pytest.exe tests\\test_transform_data3.py"
                        } else if (env.BRANCH_NAME == 'feature4-some-few-transformation') {
                            bat "${venvPath}\\pytest.exe tests\\test_transform_data4.py"
                        } else if (env.BRANCH_NAME == 'feature5-LLM-usage') {
                            echo "No dedicated transform tests for feature5, skipping transform tests"
                        } else {
                            echo "Unknown branch, running general tests"
                            bat "${venvPath}\\pytest.exe tests\\"
                        }
                    }
                }
            }
        }

        stage('Run Pipeline') {
            steps {
                script {
                    dir(pwd()) {
                        if (env.BRANCH_NAME == 'feature1-data-extraction-and-table-creation') {
                            bat "${venvPath}\\python.exe -m pipelines.pipeline1"
                        } else if (env.BRANCH_NAME == 'feature2-3-tables-transformation') {
                            bat "${venvPath}\\python.exe -m pipelines.pipeline2"
                        } else if (env.BRANCH_NAME == 'feature3-another-3-tables-transformation') {
                            bat "${venvPath}\\python.exe -m pipelines.pipeline3"
                        } else if (env.BRANCH_NAME == 'feature4-some-few-transformation') {
                            bat "${venvPath}\\python.exe -m pipelines.pipeline4"
                        } else if (env.BRANCH_NAME == 'feature5-LLM-usage') {
                            bat "${venvPath}\\python.exe -m pipelines.pipeline5"
                        } else {
                            error "Unknown branch: ${env.BRANCH_NAME}"
                        }
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
