# AWS Lambda Scraper
Short description: Project aimed at regularly (at scheduled intervals) scraping Pinnacle and Swiss Odds Provider Swisslos/Sporttip Odds using AWS services

### Motivation
Given the importance of personnel in basketball and its impact on team's win probabilities, I have become interested in tracking different sportsbooks' handling of changes to a team's lineups/the availability of its roster. Unfortunately, it is extremely difficult to acquire historical, continuous pre-game odds data. As a result, I have created the following scrapers for two sportsbooks (Pinnacle, a well known market maker, and Sporttip, a Swiss, much less sharp book).
To allow for scheduling without taking on the burden of continuous computing on my local device, I have created a Docker container to allow for the scraper to be deployed to AWS Lambda where it can run on a pre-defined schedule.

#### The Scraper
The scraper itself can be found in the app folder. It makes use of Selenium and a headless Chromedriver to find the desired data via HTML Tags.
While somewhat similar, the scrapers differ between Pinnacle and Swisslos due to difference in webpage setups.

#### The Docker File
The Docker file builds an image that can then be run to create a container, which we will eventually deploy to AWS. The code in this docker file is largely based on the following [blog post](https://aws.amazon.com/blogs/aws/new-for-aws-lambda-container-image-support/). 

#### Deployment to Lambda and connecting to an S3 Bucket 
Lastly, we would like to deploy this app to AWS Lambda, so we don't have to run it locally. To do so, you must do the following things (instructions can be found in accompanying links):
1. Install AWS CLI (if not yet installed): https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
This allows you to interact with AWS from your command line (you will use this to deploy your container)
2. Create an Elastic Container Registry to store your Docker images so they can interact with Lambda and a container can be created in the AWS cloud:
https://docs.aws.amazon.com/AmazonECR/latest/userguide/getting-started-cli.html
3. Create a lambda function:
https://docs.aws.amazon.com/lambda/latest/dg/images-create.html
4. Connect the lambda function to an S3 bucket where the scraped data can be stored:
https://repost.aws/knowledge-center/lambda-execution-role-s3-bucket

Once all this is done, you should be able to schedule your lambda function to pull odds at whatever time interval you care for (pay attention to the cost though)!

Huge thanks to [RC Coding](https://www.youtube.com/watch?v=DxET43rUkig&t=604s) and his web scraping projects, off which the majority of this code is based!
