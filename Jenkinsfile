label = "${UUID.randomUUID().toString()}"
git_project = "frames"
git_project_user = "v3io"
git_project_upstream_user = "v3io"
git_deploy_user = "iguazio-prod-git-user"
git_deploy_user_token = "iguazio-prod-git-user-token"
git_deploy_user_private_key = "iguazio-prod-git-user-private-key"

podTemplate(label: "${git_project}-${label}", inheritFrom: "jnlp-docker-golang-python37") {
    node("${git_project}-${label}") {
        pipelinex = library(identifier: 'pipelinex@development', retriever: modernSCM(
                [$class       : 'GitSCMSource',
                 credentialsId: git_deploy_user_private_key,
                 remote       : "git@github.com:iguazio/pipelinex.git"])).com.iguazio.pipelinex
        common.notify_slack {
            withCredentials([
                    string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN'),
                    string(credentialsId: 'frames-ci-url', variable: 'FRAMES_CI_URL'),
                    usernamePassword(credentialsId: 'frames-ci-user-credentials', passwordVariable: 'FRAMES_CI_PASSWORD', usernameVariable: 'FRAMES_CI_USERNAME'),
            ]) {
                github.branch(git_deploy_user, git_project, git_project_user, git_project_upstream_user, true, GIT_TOKEN) {
                    parallel(
                            'test-py': {
                                container('python37') {
                                    dir("${github.BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                                        common.shellc("pip install pipenv")
                                        common.shellc("make python-deps")
                                        sh "make test-py"
                                    }
                                }
                            },
                            'test-go': {
                                container('golang') {
                                    dir("${github.BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                                        session = '{"url":"' + FRAMES_CI_URL + '","user":"' + FRAMES_CI_USERNAME + '","password":"' + FRAMES_CI_PASSWORD + '","container":"bigdata"}'
                                        common.shellc("V3IO_SESSION='${session}' make test-go")
                                    }
                                }
                            },
                            'make lint': {
                                container('golang') {
                                    dir("${github.BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                                        sh "make lint"
                                    }
                                }
                            }
                    )
                }
                github.pr(git_deploy_user, git_project, git_project_user, git_project_upstream_user, true, GIT_TOKEN) {
                    parallel(
                            'test-py': {
                                container('python37') {
                                    dir("${github.BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                                        common.shellc("pip install pipenv")
                                        common.shellc("make python-deps")
                                         sh "make test-py"
                                    }
                                }
                            },
                            'test-go': {
                                container('golang') {
                                    dir("${github.BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                                        session = '{"url":"' + FRAMES_CI_URL + '","user":"' + FRAMES_CI_USERNAME + '","password":"' + FRAMES_CI_PASSWORD + '","container":"bigdata"}'
                                        common.shellc("V3IO_SESSION='${session}' make test-go")
                                    }
                                }
                            },
                           'make lint': {
                               container('golang') {
                                   dir("${github.BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                                       sh "make lint"
                                   }
                               }
                           }
                    )
                }
                github.release(git_deploy_user, git_project, git_project_user, git_project_upstream_user, true, GIT_TOKEN) {
                    RELEASE_ID = github.get_release_id(git_project, git_project_user, "${github.TAG_VERSION}", GIT_TOKEN)

                    parallel(
                            'build linux binaries': {
                                container('golang') {
                                    dir("${github.BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                                        common.shellc("FRAMES_TAG=${github.TAG_VERSION} GOARCH=amd64 GOOS=linux make frames-bin")
                                    }
                                }
                            },
                            'build darwin binaries': {
                                container('golang') {
                                    dir("${github.BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                                        common.shellc("FRAMES_TAG=${github.TAG_VERSION} GOARCH=amd64 GOOS=darwin make frames-bin")
                                    }
                                }
                            },
                            'build windows binaries': {
                                container('golang') {
                                    dir("${github.BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                                        common.shellc("FRAMES_TAG=${github.TAG_VERSION} GOARCH=amd64 GOOS=windows make frames-bin")
                                    }
                                }
                            },
                            'build frames': {
                                container('docker-cmd') {
                                    dir("${github.BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                                        common.shellc("FRAMES_REPOSITORY= FRAMES_TAG=${github.DOCKER_TAG_VERSION} make build")
                                    }
                                }
                            },

                    )

                    parallel(
                            'upload linux binaries': {
                                container('jnlp') {
                                    github.upload_asset(git_project, git_project_user, "framesd-${github.TAG_VERSION}-linux-amd64", RELEASE_ID, GIT_TOKEN, "${github.BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}")
                                }
                            },
                            'upload linux binaries artifactory': {
                                container('jnlp') {
                                    withCredentials([
                                            string(credentialsId: pipelinex.PackagesRepo.ARTIFACTORY_IGUAZIO[2], variable: 'PACKAGES_ARTIFACTORY_PASSWORD')
                                    ]) {
                                        common.upload_file_to_artifactory(pipelinex.PackagesRepo.ARTIFACTORY_IGUAZIO[0], pipelinex.PackagesRepo.ARTIFACTORY_IGUAZIO[1], PACKAGES_ARTIFACTORY_PASSWORD, "iguazio-devops/k8s", "framesd-${github.TAG_VERSION}-linux-amd64", "${github.BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}")
                                    }
                                }
                            },
                            'upload darwin binaries': {
                                container('jnlp') {
                                    github.upload_asset(git_project, git_project_user, "framesd-${github.TAG_VERSION}-darwin-amd64", RELEASE_ID, GIT_TOKEN, "${github.BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}")
                                }
                            },
                            'upload windows binaries': {
                                container('jnlp') {
                                    github.upload_asset(git_project, git_project_user, "framesd-${github.TAG_VERSION}-windows-amd64", RELEASE_ID, GIT_TOKEN, "${github.BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}")
                                }
                            },
                            'upload to pypi': {
                                container('python37') {
                                    release_body = github.get_release_body("frames", git_project_user, github.TAG_VERSION, GIT_TOKEN)
                                    if (release_body.startsWith("Autorelease")) {
                                        echo "Autorelease is not uploading frames py to pypi."
                                    } else if( "${github.TAG_VERSION}" != "unstable" ) {
                                        withCredentials([
                                                usernamePassword(credentialsId: "iguazio-prod-pypi-credentials", passwordVariable: 'V3IO_PYPI_PASSWORD', usernameVariable: 'V3IO_PYPI_USER')
                                        ]) {
                                            dir("${github.BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                                                FRAMES_PYPI_VERSION = sh(
                                                        script: "echo ${github.DOCKER_TAG_VERSION} | awk -F - '{print \$1}'",
                                                        returnStdout: true
                                                ).trim()
                                                common.shellc("pip install pipenv")
                                                common.shellc("make python-deps")
                                                sh "make test-py"
                                                try {
                                                    common.shellc("TRAVIS_REPO_SLUG=v3io/frames V3IO_PYPI_USER=${V3IO_PYPI_USER} V3IO_PYPI_PASSWORD=${V3IO_PYPI_PASSWORD} TRAVIS_TAG=${FRAMES_PYPI_VERSION} make pypi")
                                                } catch (err) {
                                                    unstable("Failed uploading to pypi")
                                                    // Do not continue stages
                                                    throw err
                                                }
                                            }
                                        }
                                    } else {
                                        echo "Uploading to pypi only stable version"
                                    }
                                }
                            },
                    )

                    container('docker-cmd') {
                        dockerx.images_push_multi_registries(["frames:${github.DOCKER_TAG_VERSION}"], [pipelinex.DockerRepo.ARTIFACTORY_IGUAZIO, pipelinex.DockerRepo.DOCKER_HUB, pipelinex.DockerRepo.QUAY_IO])
                    }
                }
            }
        }
    }
}
