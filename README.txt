# Hebrew N-gram Word Prediction System

**Authors:** 
- May Hadadi (hadadim@post.bgu.ac.il)
- Asaf Hacmon**

Final Grade: 94/100**

## Project Overview

This project implements a MapReduce system for Hebrew word prediction using Google's n-gram dataset. The system calculates conditional probabilities for trigrams (three consecutive words) to predict the most likely next word given a pair of preceding words.

### Key Features

- Processes Google's Hebrew 1-gram, 2-gram, and 3-gram datasets
- Filters out Hebrew stop words
- Calculates conditional probabilities using a backoff smoothing technique
- Orders results by word pairs and descending probability
- Implements full MapReduce workflow on Amazon EMR

## Architecture

The system is structured as a multi-step MapReduce workflow:

1. **Step 1:** Process unigrams (1-grams) to extract C0, C1, and N1 statistics
2. **Step 2:** Process bigrams (2-grams) to extract C2 and N2 statistics
3. **Step 3:** Process trigrams (3-grams) to extract N3 statistics
4. **Step 4a:** Aggregate statistics from previous steps
5. **Step 4b:** Calculate probabilities using the Thede & Harper formula
6. **Step 5:** Sort results by word pairs (ascending) and probability (descending)

## Setup Instructions

### Prerequisites

- AWS Account with permissions for S3, EMR, and EC2
- AWS CLI configured with appropriate credentials
- Java 8 or newer
- Maven

### S3 Bucket Structure

Create an S3 bucket with the following folder structure:
```
bucket-name/
├── jars/        # Contains JAR files for all steps
├── logs/        # EMR logs
├── output/      # Step output directories
└── resources/   # Contains heb-stopwords.txt
```

### Building the Project

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/hebrew-word-prediction.git
   cd hebrew-word-prediction
   ```

2. Build the JAR files:
   ```bash
   mvn clean package
   ```

3. Upload JAR files to S3:
   ```bash
   aws s3 cp target/Step1.jar s3://yourbucket/jars/
   aws s3 cp target/Step2.jar s3://yourbucket/jars/
   aws s3 cp target/Step3.jar s3://yourbucket/jars/
   aws s3 cp target/Step4a.jar s3://yourbucket/jars/
   aws s3 cp target/Step4b.jar s3://yourbucket/jars/
   aws s3 cp target/Step5.jar s3://yourbucket/jars/
   aws s3 cp target/App.jar s3://yourbucket/jars/
   aws s3 cp resources/heb-stopwords.txt s3://yourbucket/resources/
   ```

### Running the Project

To run the complete pipeline:

```bash
java -jar target/App.jar
```

This will:
1. Connect to AWS EMR
2. Create a cluster with 6 instances
3. Execute all MapReduce steps in sequence
4. Store results in the S3 output directory

## Implementation Details

### Data Flow Logic

1. **Step 1:** Extract unigram occurrences and total corpus size (C0)
2. **Step 2:** Extract bigram occurrences (C2, N2)
3. **Step 3:** Extract trigram occurrences (N3)
4. **Step 4a:** Organize data to prepare for probability calculation by:
   - Passing C0 to Step 4b
   - Processing unigrams, bigrams, and trigrams appropriately
   - Combining relevant statistics for each trigram
5. **Step 4b:** Calculate trigram probabilities using the formula:
   ```
   P(w3|w1,w2) = k3 * (N3/C2) + (1-k3) * k2 * (N2/C1) + (1-k3) * (1-k2) * (N1/C0)
   ```
   Where k2 and k3 are calculated based on occurrence counts
6. **Step 5:** Sort results by (w1,w2) pairs and probability

### Performance Analysis

#### Key-Value Pairs Statistics (with Combiner)
- Step 1: Reduced from 88,045,604 to 639,718 records (-99.3%)
- Step 2: Reduced from 116,105,826 to 2,597,858 records (-97.8%)
- Step 3: Reduced from 14,240,870 to 360,173 records (-97.5%)

#### Scalability
- Google n-grams with 3 mappers: ~26 minutes
- Google n-grams with 6 mappers: ~16 minutes (38% improvement)
- Example n-grams with 3 mappers: ~12 minutes
- Example n-grams with 6 mappers: ~10 minutes (17% improvement)

### Sample Results

Here are some interesting word pairs with their top 5 most probable next words:

**אברהם אבינו:**
1. הראשון
2. אמר
3. שהוא
4. אלא 
5. עשה

**כך נוצר:**
1. מצב
2. קשר
3. הרושם
4. גם
5. מתח

**בקונגרס העולמי:**
1. הרביעי
2. החמישי
3. השביעי
4. השמיני
5. השלישי

**מאז ימי:**
1. הביניים
2. בראשית
3. המלחמה
4. קדם
5. מלחמת

**הגדול ביותר:**
1. בקרב
2. בתחום
3. שלה
4. מאז
5. בירושלים

**צריך אני:**
1. לעשות
2. לדעת
3. להודות
4. לספר
5. לכתוב

## Notes

- The system successfully filters Hebrew stop words to improve prediction quality
- The implementation uses combiners to significantly reduce network traffic between mappers and reducers
- The word predictions follow logical semantic patterns in Hebrew
- For optimal performance on AWS EMR, we recommend using at least 6 m4.large instances