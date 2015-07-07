#!/bin/sh

echo `date` starting script...

wd=$HOME'/eventcube/sh'
run_sql="psql -h 10.223.192.6 -p 5432 analytics etl -f "

# Know which events and users to include
echo `date` running testevents.sql; $run_sql $wd"/../sql/testevents.sql"
echo `date` running dimusers.sql; $run_sql $wd"/../sql/dimusers.sql"

# The facts
echo `date` running factsessions.sql; $run_sql $wd"/../sql/factsessions.sql"
echo `date` running factposts.sql; $run_sql $wd"/../sql/factposts.sql" 
echo `date` running factlikes.sql; $run_sql $wd"/../sql/factlikes.sql" 
echo `date` running factcomments.sql; $run_sql $wd"/../sql/factcomments.sql" 
echo `date` running factbookmarks.sql; $run_sql $wd"/../sql/factbookmarks.sql" 
echo `date` running factfollows.sql; $run_sql $wd"/../sql/factfollows.sql" 
echo `date` running factcheckins.sql; $run_sql $wd"/../sql/factcheckins.sql" 
echo `date` running factratings.sql; $run_sql $wd"/../sql/factratings.sql" 
echo `date` running factsurveys.sql; $run_sql $wd"/../sql/factsurveys.sql" 

# User attributes
echo `date` running dimuserbinaryversion.sql; $run_sql $wd"/../sql/dimuserbinaryversion.sql" 
echo `date` running dimuserdevicetype.sql; $run_sql $wd"/../sql/dimuserdevicetype.sql" 
echo `date` running dimusersocialnetworks.sql; $run_sql $wd"/../sql/dimusersocialnetworks.sql" 

# Event attributes
echo `date` running dimeventbinaryversion.sql; $run_sql $wd"/../sql/dimeventbinaryversion.sql" 
echo `date` running dimevents.sql; $run_sql $wd"/../sql/dimevents.sql" 
echo `date` running dimitems.sql; $run_sql $wd"/../sql/dimitems.sql" 
echo `date` running dimsurveys.sql; $run_sql $wd"/../sql/dimsurveys.sql" 
echo `date` running dimeventssfdc.sql; $run_sql $wd"/../sql/dimeventssfdc.sql" 

# Da cubes
echo `date` running usercubesummary.sql; $run_sql $wd"/../sql/usercubesummary.sql" 
echo `date` running eventcubesummary.sql; $run_sql $wd"/../sql/eventcubesummary.sql" 

# User cube by date
# (no longer running as it is a remnant of a previous need)
# echo `date` running usercubedaily.sql; $run_sql $wd"/../sql/usercubedaily.sql" 

echo `date` finished script...

