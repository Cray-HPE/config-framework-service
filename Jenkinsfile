
@Library('dst-shared@master') _

dockerBuildPipeline {
    repository = "cray"
    imagePrefix = "cray"
    app = "cfs"
    name = "cray-cfs"
    description = "Cray management system configuration framework service"
    product = "csm"

    githubPushRepo = "Cray-HPE/config-framework-service"
    /*
        By default all branches are pushed to GitHub

        Optionally, to limit which branches are pushed, add a githubPushBranches regex variable
        Examples:
        githubPushBranches =  /master/ # Only push the master branch
        
        In this case, we push bugfix, feature, hot fix, master, and release branches
    */
    githubPushBranches =  /(bugfix\/.*|feature\/.*|hotfix\/.*|master|release\/.*)/ 
}
