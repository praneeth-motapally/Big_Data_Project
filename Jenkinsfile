def venvPath = 'D:\\Big_Data_Project\\.venv\\Scripts'

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
                // Running pip from the root of the workspace
                bat "${venvPath}\\pip.exe install -r requirements.txt"
            }
        }

        stage('Run Tests') {
            steps {
                echo "Running tests"
                script {
                    // Navigate to the workspace root before running
                    // and use relative paths
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
                    // Navigate to the workspace root before running
                    // and use relative paths
                    dir(pwd()) {
                        if (env.BRANCH_NAME == 'feature1-data-extraction-and-table-creation') {
                            bat "${venvPath}\\python.exe pipelines\\pipeline1.py"
                        } else if (env.BRANCH_NAME == 'feature2-3-tables-transformation') {
                            bat "${venvPath}\\python.exe pipelines\\pipeline2.py"
                        } else if (env.BRANCH_NAME == 'feature3-another-3-tables-transformation') {
                            bat "${venvPath}\\python.exe pipelines\\pipeline3.py"
                        } else if (env.BRANCH_NAME == 'feature4-some-few-transformation') {
                            bat "${venvPath}\\python.exe pipelines\\pipeline4.py"
                        } else if (env.BRANCH_NAME == 'feature5-LLM-usage') {
                            bat "${venvPath}\\python.exe pipelines\\pipeline5.py"
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