import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

/**
 * Main Application (App.java)
 * Purpose: Orchestrates the entire MapReduce workflow for processing n-grams
 * 
 * Input: N/A (Orchestration class)
 * Output: Creates and manages EMR cluster and job flow
 */
public class App {
        public static AWSCredentialsProvider credentialsProvider;
        public static AmazonElasticMapReduce emr;
        public static String step1op = "s3://oursteps/output/step1/";
        public static String step2op = "s3://oursteps/output/step2/";
        public static String step3op = "s3://oursteps/output/step3/";
        public static String step4aop = "s3://oursteps/output/step4a/";
        public static String step4bop = "s3://oursteps/output/step4b/";
        public static String step5op = "s3://oursteps/output/step5/";
        public static String stopWords = "s3://oursteps/resources/heb-stopwords.txt";

        public static void main(String[] args) {
                // Initialize AWS credentials and clients
                credentialsProvider = new ProfileCredentialsProvider();
                System.out.println("[INFO] Connecting to AWS");

                emr = AmazonElasticMapReduceClientBuilder.standard()
                                .withCredentials(credentialsProvider)
                                .withRegion("us-east-1")
                                .build();

                System.out.println("[INFO] Listing existing clusters:");
                System.out.println(emr.listClusters());

                // Step 1: Process unigrams
                HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                                .withJar("s3://oursteps/jars/Step1.jar")
                                .withMainClass("Step1")
                                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data",
                                        step1op, 
                                        stopWords        
                                );

                StepConfig stepConfig1 = new StepConfig()
                                .withName("Process Unigrams")
                                .withHadoopJarStep(step1)
                                .withActionOnFailure("TERMINATE_JOB_FLOW");

                // Step 2: Process bigrams
                HadoopJarStepConfig step2 = new HadoopJarStepConfig()
                                .withJar("s3://oursteps/jars/Step2.jar")
                                .withMainClass("Step2")
                                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data",
                                        step2op,
                                        stopWords 
                                );

                StepConfig stepConfig2 = new StepConfig()
                                .withName("Process Bigrams")
                                .withHadoopJarStep(step2)
                                .withActionOnFailure("TERMINATE_JOB_FLOW");

                // Step 3: Process trigrams
                HadoopJarStepConfig step3 = new HadoopJarStepConfig()
                                .withJar("s3://oursteps/jars/Step3.jar")
                                .withMainClass("Step3")
                                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data",
                                        step3op,   
                                        stopWords                                                   
                                );

                StepConfig stepConfig3 = new StepConfig()
                                .withName("Process Trigrams")
                                .withHadoopJarStep(step3)
                                .withActionOnFailure("TERMINATE_JOB_FLOW");

                HadoopJarStepConfig Step4a = new HadoopJarStepConfig()
                                .withJar("s3://oursteps/jars/Step4a.jar")
                                .withMainClass("Step4a")
                                .withArgs( step1op,
                                        step2op,
                                        step3op,
                                        step4aop
                                );

                StepConfig stepFourA = new StepConfig()
                                .withName("Step4a")
                                .withHadoopJarStep(Step4a)
                                .withActionOnFailure("TERMINATE_JOB_FLOW");
                /*
                 * step 4b
                 * Calculate the desired probability
                 * (all needed data is supplied in this step, using a local hashmap for 1-gram).
                 */
                HadoopJarStepConfig Step4b = new HadoopJarStepConfig()
                                .withJar("s3://oursteps/jars/Step4b.jar")
                                .withMainClass("Step4b")
                                .withArgs(step4aop,
                                          step4bop
                                        );

                StepConfig stepFourB = new StepConfig()
                                .withName("Step4b")
                                .withHadoopJarStep(Step4b)
                                .withActionOnFailure("TERMINATE_JOB_FLOW");

                // Step 5: Sort final results
                HadoopJarStepConfig step5 = new HadoopJarStepConfig()
                                .withJar("s3://oursteps/jars/Step5.jar")
                                .withMainClass("Step5")
                                .withArgs(step4bop,
                                          step5op       
                                );

                StepConfig stepConfig5 = new StepConfig()
                                .withName("Sort Results")
                                .withHadoopJarStep(step5)
                                .withActionOnFailure("TERMINATE_JOB_FLOW");

                // Configure EMR cluster
                JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                                .withInstanceCount(6) // Adjust as needed
                                .withMasterInstanceType(InstanceType.M4Large.toString())
                                .withSlaveInstanceType(InstanceType.M4Large.toString())
                                .withHadoopVersion("2.9.2")
                                .withEc2KeyName("vockey")
                                .withKeepJobFlowAliveWhenNoSteps(false)
                                .withPlacement(new PlacementType("us-east-1a"));

                // Create job flow request
                System.out.println("[INFO] Setting up job flow with steps");
                RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                                .withName("MapReduce Project")
                                .withInstances(instances)
                                .withSteps(stepConfig1, stepConfig2, stepConfig3, stepFourA, stepFourB, stepConfig5)
                                .withLogUri("s3://oursteps/logs/")
                                .withServiceRole("EMR_DefaultRole")
                                .withJobFlowRole("EMR_EC2_DefaultRole")
                                .withReleaseLabel("emr-5.11.0");

                // Run job flow
                RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
                String jobFlowId = runJobFlowResult.getJobFlowId();
                System.out.println("[INFO] Ran job flow with ID: " + jobFlowId);
        }
}
