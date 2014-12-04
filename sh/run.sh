#!/bin/sh

echo `date` starting script...

wd="/home/anguyen/eventcube/sql/"
get_csv="python /home/anguyen/tools/get_csv.py "

# Know which events and users to include
echo `date` running testevents.sql; $get_csv $wd"testevents.sql" no_output
echo `date` running dimusers.sql; $get_csv $wd"dimusers.sql" no_output

# The facts
echo `date` running factsessions.sql; $get_csv $wd"factsessions.sql" no_output
echo `date` running factposts.sql; $get_csv $wd"factposts.sql" no_output
echo `date` running factlikes.sql; $get_csv $wd"factlikes.sql" no_output
echo `date` running factcomments.sql; $get_csv $wd"factcomments.sql" no_output
echo `date` running factbookmarks.sql; $get_csv $wd"factbookmarks.sql" no_output
echo `date` running factfollows.sql; $get_csv $wd"factfollows.sql" no_output
echo `date` running factcheckins.sql; $get_csv $wd"factcheckins.sql" no_output
echo `date` running factratings.sql; $get_csv $wd"factratings.sql" no_output
echo `date` running factsurveys.sql; $get_csv $wd"factsurveys.sql" no_output

# User attributes
echo `date` running dimuserbinaryversion.sql; $get_csv $wd"dimuserbinaryversion.sql" no_output
echo `date` running dimuserdevicetype.sql; $get_csv $wd"dimuserdevicetype.sql" no_output
echo `date` running dimuserdevicetype.sql; $get_csv $wd"dimusersocialnetworks.sql" no_output

# Event attributes
echo `date` running dimeventbinaryversion.sql; $get_csv $wd"dimeventbinaryversion.sql" no_output
echo `date` running dimevents.sql; $get_csv $wd"dimevents.sql" no_output
echo `date` running dimitems.sql; $get_csv $wd"dimitems.sql" no_output
echo `date` running dimsurveys.sql; $get_csv $wd"dimsurveys.sql" no_output

# Da cubes
echo `date` running usercubesummary.sql; $get_csv $wd"usercubesummary.sql" no_output
echo `date` running eventcubesummary.sql; $get_csv $wd"eventcubesummary.sql" no_output

# User cube by date
echo `date` running usercubedaily.sql; $get_csv $wd"usercubedaily.sql" no_output

echo `date` finished script...

