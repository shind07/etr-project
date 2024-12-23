# etr-project

## Prompt

1. Convert the R code to python
2. Make the script run as quickly as possible
3. We are currently pushing this data into a google sheet and have built a calculator that queries the data (e.g., how often does AJ Brown get more than 50 Receiving Yards?). This method has its limitations and we would like to eventually build a new calculator/tool/app to make these calculations more quick/efficient. After working through 1 and 2, what ideas do you have for building a better version of this calculator? It could be within a google sheet or something else. 
4. If time allows, please extend your thoughts on #3 to a calculator/tool/app that could price parlays (i.e., how often does AJ Brown have more than 50 Receiving Yards AND Saquon Barkley has under 75 rushing yards?).

A few logistical notes:
- Please reply to this email immediately to confirm you have what you need
- In the last section of the script (~line 126), you may need to log in to a google account / browser in order to access a google sheet. There is further instruction within the script. Other than that, we expect the R script to run smoothly as provided. We're not intending to trick you with anything in there, so feel free to reach out if you encounter serious issues.
- You may use any methods you'd like for #1 and #2, but please keep in mind that, eventually, we would like this to fit nicely on AWS in some capacity. 
On #3 and #4, we're just asking for a paragraph or two description - we DO NOT EXPECT you to actually implement these ideas right now. 

## Decision Journal

### Initial Steps

- Lets get a jupyter notebook going to play around with things and explore the data
- Need to install and set up R
- Run the script in R for a baseline time

## Plan
- First, get it working in python
- Then, make it fast
- Finally, make it pretty
- Move it into Docker so it runs easily
- Set up makefile
- Run it on AWS?

going to skip google sheets for now

## Stats
- R baseline - roughly a minute or so, time by hand
- Python: took about 17 seconds, 9 GB of RAM per Docker


# 3 - improvement suggestions
- RAG approach? LLM?
- google sheets - depends who the end user is? could do a custom ui? effort vs reward?

- skipping unit tests - not letting perfect be the enemy of good
treating this like a hackathon wher eI dont get style points