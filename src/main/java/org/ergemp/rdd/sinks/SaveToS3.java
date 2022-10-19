package org.ergemp.rdd.sinks;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Calendar;
import java.util.UUID;

/*
* in order to access s3 first;
* import jars under the libs directory
* */

public class SaveToS3 {
    public static void main(String[] args){
        writeToBucket("this is test2", "annen");
        writeToBucketPartitioned("this is test2", "annen", System.currentTimeMillis());
    }

    public static void writeToBucket(String gStr, String gFolder) {
        try
        {
            String source = "xennio" + "hdfssink" + System.currentTimeMillis();
            byte[] bytes = source.getBytes();
            UUID uuid = UUID.nameUUIDFromBytes(bytes);

            org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
            configuration.set("fs.s3a.access.key", "s3AccessToken");
            configuration.set("fs.s3a.secret.key", "s3AccessSecret");
            configuration.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem");

            createBucket("s3BucketName" );

            Path filePath = new Path("s3a://" +  "s3BucketName" + "/" + gFolder +  "/" + uuid + ".txt");
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(new URI("s3a://" + "s3BucketName"), configuration);
            //org.apache.hadoop.fs.FileSystem fs = filePath.getFileSystem(configuration);
            FSDataOutputStream os = fs.create(filePath,true);

            BufferedWriter bw = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
            bw.write(gStr);
            bw.flush();

            os.close();
        }
        catch(Exception ex){
            ex.printStackTrace();
        }
        finally{
        }
    }

    public static void writeToBucketPartitioned(String gStr, String gFolder, Long epochTime) {
        try {
            Calendar customCal = Calendar.getInstance();
            customCal.setTimeInMillis(epochTime);

            String year = String.valueOf(customCal.get(Calendar.YEAR));
            String month = String.valueOf(customCal.get(Calendar.MONTH)+1);
            String day = String.valueOf(customCal.get(Calendar.DAY_OF_MONTH));
            String hour = String.valueOf(customCal.get(Calendar.HOUR_OF_DAY));

            String source = "xennio" + "hdfssink" + System.currentTimeMillis();
            byte[] bytes = source.getBytes();
            UUID uuid = UUID.nameUUIDFromBytes(bytes);

            org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
            configuration.set("fs.s3a.access.key", "s3AccessToken");
            configuration.set("fs.s3a.secret.key", "s3AccessSecret");
            configuration.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem");

            createBucket("s3BucketName");

            Path filePath = new Path("s3a://" + "s3BucketName" + "/" + gFolder + "/year="+year+"/month="+month+"/day="+day+"/hour="+hour+"/" + uuid + ".txt");
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(new URI("s3a://" + "s3BucketName"), configuration);
            //org.apache.hadoop.fs.FileSystem fs = filePath.getFileSystem(configuration);
            FSDataOutputStream os = fs.create(filePath,true);

            BufferedWriter bw = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
            bw.write(gStr);
            bw.flush();

            os.close();
        }
        catch(Exception ex){
            ex.printStackTrace();
        }
        finally{
        }
    }

    public static void createBucket(String gBucketName){
        BasicAWSCredentials creds = new BasicAWSCredentials("s3AccessToken", "s3AccessSecret");
        AmazonS3 s3Client = new AmazonS3Client(creds);
        if (!s3Client.doesBucketExist(gBucketName))
        {
            s3Client.createBucket(gBucketName);
        }
    }

    public static void deleteBucket(String gBucketName){
        BasicAWSCredentials creds = new BasicAWSCredentials("s3AccessToken", "s3AccessSecret");
        AmazonS3 s3Client = new AmazonS3Client(creds);
        s3Client.deleteBucket(gBucketName);
    }

    public static void createFolder(String bucketName, String folderName, AmazonS3 client) {

        // create meta-data for your folder and set content-length to 0
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);

        // create empty content
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

        // create a PutObjectRequest passing the folder name suffixed by /
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, folderName + "/", emptyContent, metadata);

        // send request to S3 to create folder
        client.putObject(putObjectRequest);
    }
}
