package org.ergemp.sql.sources;

public class CreateDfFromS3MultipleAccounts {
    public static void main(String[] args) {

        /*
        ref: https://community.cloudera.com/t5/Support-Questions/spark-read-from-different-account-s3-and-write-to-my-account/td-p/227960

        * with the S3A connector you can use per-bucket configuration options
        * to set a different username/pass for the remote bucket

        fs.s3a.bucket.myaccounts3.access.key=AAA12

        fs.s3a.bucket.myaccounts3.secret.key=XXXYYY

        Then when you read or write s3a://myaccounts3/ then these specific username/passwords are used.
        * For other S3A buckets, the default ones are picked up:

        fs.s3a.access.key=BBBB

        fs.s3a.secret.key=ZZZZZ

        Please switch to using the s3a:// connector everywhere:
        * its got much better performance and functionality than the older S3N one,
        * which has recently been removed entirely.
        * */
    }
}
