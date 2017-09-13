node() {
    stage('Setup') {
        java.jdk8()
        github.clone('est-s3ninja')
    }

    stage('Build') {
        mvnw.clean()
        mvnw.install()
    }

    if (branch.isMaster()) {
        stage('Docker') {
            sh 'cp target/s3ninja-*.zip docker/s3ninja.zip'
            docker.withRegistry(env.DOCKER_REGISTRY_URL, env.DOCKER_CREDENTIALS_ID) {
                def image = docker.build("est/mule-platform/s3ninja", 'docker')

                image.push('latest')
                image.push(env.BUILD_NUMBER)
            }
        }
    }
}