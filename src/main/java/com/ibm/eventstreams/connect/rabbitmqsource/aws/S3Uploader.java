package com.ibm.eventstreams.connect.rabbitmqsource.aws;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;



import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;




public class S3Uploader {

    final Region region = Region.AP_SOUTHEAST_2;
    final S3Client s3Client = S3Client.builder().region(region).build();

    private final String bucketName;
    private final String folder;
    private final String payloadLocation;

    public S3Uploader(String bucketName, String folder){
        this.bucketName = bucketName;
        this.folder = folder;
        this.payloadLocation = "{'payloadLocation' : 'https://" + bucketName + ".s3.amazonaws.com/"; 
    }

    /*
     * Returns full qualified S3 location of the file
     * 
     */
    public String uploadData(String messageBody) throws SdkException{
        String fileKey = folder + "/" + UUID.randomUUID().toString();
        PutObjectRequest fileUploadRequest = 
            PutObjectRequest.builder()
            .bucket(bucketName)
            .key(fileKey)
            .build();
        
        ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(messageBody);

        try{
            s3Client.putObject(fileUploadRequest, RequestBody.fromByteBuffer(byteBuffer));
            return payloadLocation + fileKey + "'}";
        } catch (SdkException e) {
            String errorMessage = "Failed to store the message content in an S3 object." + e.getMessage();
            System.out.println(errorMessage);
            throw SdkException.create(errorMessage, e);
        }
        


    }

    
}
