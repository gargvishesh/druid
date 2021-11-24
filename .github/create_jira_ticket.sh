#!/bin/bash

helpFunction()
{
   echo ""
   echo "Usage: $0 -d /path/to/druid -c previouscommit -j jira_email:token -f fix_version"
   echo -e "\t-d Path where imply druid is checked out"
   echo -e "\t-c Previous Commit, if defined previos monthly release version will be ignored"
   echo -e "\t-j Jira user email and api token eg:- abc@xyz.com:123xyz"
   echo -e "\t-f Monthly Fix Release Version"
   exit 1 # Exit script after printing help
}

while getopts "d:c:j:f:" opt
do
   case "$opt" in
      d ) DRUID_DIR="$OPTARG" ;;
      c ) PREV_COMMIT="$OPTARG" ;;
      j ) JIRA_CRED="$OPTARG" ;;
      f ) FIX_VERSION="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

# Print helpFunction in case any of the parameter is empty
if [ -z "$PREV_COMMIT" ] || [ -z "$JIRA_CRED" ] || [ -z "$FIX_VERSION" ]
then
   echo "Some or all of the parameters are empty";
   helpFunction
fi

if [ -z "$DRUID_DIR" ]
then
   DRUID_DIR=$(pwd)
fi

# Get all the commits from Master Merged to Monthly
COMMITS=$(cd $DRUID_DIR/; git log --pretty=format:%h $PREV_COMMIT..monthly)
#Add upstream to verify if the commit is from upstream or not
cd $DRUID_DIR;git remote add upstream https://github.com/apache/druid.git;git remote update

for commit in `echo $COMMITS`
do
   if `cd $DRUID_DIR;git branch -r --contains $commit| grep -q upstream/master`
      then
         PR=$(cd $DRUID_DIR;git log -n 1  --pretty=format:'%s' $commit|awk -F"[()]" '{print $2}'|awk -F'#' '{print $2}')
         echo $PR
         URL=https://implydata.atlassian.net/rest/api/3/search?jql=
         URL="$URL""PR%3Dhttps%3A%5Cu002f%5Cu002fgithub.com%5Cu002fapache%5Cu002fdruid%5Cu002fpull%5Cu002f""$PR"
         URL="$URL""%20or%20PR%3Dhttps%3A%5Cu002f%5Cu002fgithub.com%5Cu002fapache%5Cu002fdruid%5Cu002fpull%5Cu002f""$PR""%5Cu002f"
         URL="$URL""%20or%20%22Apache%20Druid%20Issue%22%3Dhttps%3A%5Cu002f%5Cu002fgithub.com%5Cu002fapache%5Cu002fdruid%5Cu002fpull%5Cu002f""$PR"
         URL="$URL""%20or%20%22Apache%20Druid%20Issue%22%3Dhttps%3A%5Cu002f%5Cu002fgithub.com%5Cu002fapache%5Cu002fdruid%5Cu002fpull%5Cu002f""$PR""%5Cu002f"
         # Verify if we have a jira for the PR
         JIRA=$(curl -s --request GET --url "$URL" --user $JIRA_CRED --header 'Accept: application/json'|jq -r '.total')
         if [ $JIRA -eq 0 ]
            then
               echo "Creating jira Task for "$PR
               cp $DRUID_DIR/.github/jira_task.json.template $DRUID_DIR/jira_task.json
               PRDESC=$(git log --pretty=format:%s --grep=\(\#$PR\))
               PRDESC="$(echo "$PRDESC"|tr -dc '[:alnum:] :#()')"
               sed -i.bak 's@%%PR%%@'"$PR"'@g' $DRUID_DIR/jira_task.json
               sed -i.bak 's@%%PRDESC%%@'"$PRDESC"'@g' $DRUID_DIR/jira_task.json
               sed -i.bak 's@%%FIXVERSION%%@'"$FIX_VERSION"'@g' $DRUID_DIR/jira_task.json
               # Create a jira task for the PR
               curl --request POST --url 'https://implydata.atlassian.net/rest/api/3/issue'  --user $JIRA_CRED \
               --header 'Accept: application/json'  --header 'Content-Type: application/json' \
               --data  @$DRUID_DIR/jira_task.json
               rm $DRUID_DIR/jira_task.json
         else
               echo $PR" already has a ticket"
         fi
   fi
done

