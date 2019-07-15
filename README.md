hornetq-oneoffs
===============

this repository is a place holder for one-off branches. 

The master should be always empty.


naming conventions
==================

You should always use oneoff-ORIGINAL_TAG_NAME-NEW_JIRA


Example:

An one off of the tag HORNETQ_2_2_5_EAP_GA. We have created a one-off JIRA jbpapp_7710, so the branch name will be oneoff-HORNETQ_2_2_5_EAP_GA-JBPAPP_7710.

Notice that I'm only using the character - twice separating the branch-name.

This is to make clear what branch it was made off.



Notice: if later you need to create another one-off in top of 2.2.5.EAP.GA, you need to do it again from 2.2.5.eap.ga and import the commits you need. You never do a one-off of a one-off branch (that would be really messy and hard to manage).




pushing a new branch
====================

You just push straight from your working copy

You add the remote:

git remote add one-offs git@github.com:hornetq/hornetq-oneoffs.git

git push one-offs BRANCH-NAME_USING_CONVENTIONS-WITHJIRA
