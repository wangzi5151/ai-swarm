pluginManagement {
    repositories {
        maven { url = uri("/root/maven/localMvnRepository") }
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        maven { url = uri("/root/maven/localMvnRepository") }
        google()
        mavenCentral()
    }
}

rootProject.name = "AICollabForge"
include(":app")
