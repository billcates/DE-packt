#readme file

Data Engineering Task - Packt

The project is focused on extracting and analyzing data from the Stack Overflow platform. Here's a summary of the project:

Extraction:

1. Trending Tags Extraction:
   - The `fetch_trending_tags` function fetches the popular tags from Stack Overflow within the last month using the Stack Exchange API.
   - The extracted tags are stored in a pandas DataFrame.

2. Emerging Technologies Analysis:
   - The `check_emerging_technologies` function reads the tags from previously extracted CSV files stored locally or in an S3 bucket.
   - The function compares the latest tags with tags from previous files to identify emerging technologies.
   - The emerging technologies (tags) are returned as a list.

3. Popular Questions Extraction:
   - The `fetch_popular_questions` function retrieves popular questions from Stack Overflow within the last month based on the number of votes.
   - The function collects the question ID, title, and link for each question and stores them in a pandas DataFrame.

4. Top Answered Questions by Tag:
   - The `fetch_top_answered_questions` function retrieves the top answered questions for a list of specified tags.
   - For each tag, the function fetches the questions with the most votes and retrieves the question ID, title, link, and answer count.
   - The collected information is stored in a pandas DataFrame.
   - The DataFrame is then saved to a CSV file using the `save_file` function.

Once the data is extracted, it is pushed to s3 bucket ('de-packt-stack-exchange-bucket').

We are storing the files in s3 so that we can leverage glue crawler and query the data on top of s3.
By doing this we are making the storage costs cheap and we can change our code such that it works in AWS lambda function and we can trigger the job as per business need or we can schedule it in AWS Eventbridge such that it runs on a scheduled interval. By doing this, we only pay for what we use,how much time our lambda runs. 

Analysis of Data:

Once our initial files are placed in s3, we can run glue crawler on the s3 bucket and create a glue data catalog table. So that we can do analysis on Athena or we can create a external Schema in Redshift and we can analyze our data in redshift.

proposed.png is the proposed architecture diagram.

