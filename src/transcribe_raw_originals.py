import boto3
import os
import time
import argparse
from datetime import datetime
from botocore.exceptions import ClientError

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket"""
    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(f"Error uploading file: {e}")
        return False
    return True

def start_transcription_job(job_name, file_uri, output_bucket, output_key):
    """Start an Amazon Transcribe job"""
    transcribe_client = boto3.client('transcribe')
    try:
        response = transcribe_client.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': file_uri},
            MediaFormat='m4a',
            LanguageCode='en-US',
            OutputBucketName=output_bucket,
            OutputKey=output_key,
            Settings={
                'ShowSpeakerLabels': False,
                'ChannelIdentification': False
            }
        )
        return response['TranscriptionJob']['TranscriptionJobName']
    except ClientError as e:
        print(f"Error starting transcription job: {e}")
        return None

def get_transcription_job_status(job_name):
    """Get the status of a transcription job"""
    transcribe_client = boto3.client('transcribe')
    try:
        response = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
        return response['TranscriptionJob']['TranscriptionJobStatus']
    except ClientError as e:
        print(f"Error getting transcription job status: {e}")
        return None

def download_file(bucket, object_name, file_name):
    """Download a file from an S3 bucket"""
    s3_client = boto3.client('s3')
    try:
        s3_client.download_file(bucket, object_name, file_name)
    except ClientError as e:
        print(f"Error downloading file: {e}")
        return False
    return True

def extract_raw_transcript(input_file, output_file):
    """Extract raw transcript from the JSON file"""
    import json
    
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    transcript = data['results']['transcripts'][0]['transcript']
    
    with open(output_file, 'w') as f:
        f.write(transcript)

def main():
    parser = argparse.ArgumentParser(description="Transcribe an m4a file using Amazon Transcribe")
    parser.add_argument("--input_file", required=True, help="Path to the local m4a file")
    parser.add_argument("--output_file", required=True, help="Path to save the transcription output")
    args = parser.parse_args()

    # Parameters
    local_file_path = args.input_file
    output_file_path = args.output_file
    s3_bucket_name = 'sat-ddc'
    if not s3_bucket_name:
        print("Error: S3_BUCKET_NAME environment variable is not set")
        return

    file_name = os.path.basename(local_file_path)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    s3_object_name = f"raw_audio/{file_name}"
    transcription_job_name = f"{os.path.splitext(file_name)[0]}_{timestamp}"
    transcription_output_key = f"raw_audio_transcriptions/{transcription_job_name}.json"

    # Upload the file to S3
    if upload_file(local_file_path, s3_bucket_name, s3_object_name):
        print(f"Successfully uploaded {local_file_path} to {s3_bucket_name}/{s3_object_name}")
    else:
        print("File upload failed")
        return

    # Start the transcription job
    s3_uri = f"s3://{s3_bucket_name}/{s3_object_name}"
    job_name = start_transcription_job(transcription_job_name, s3_uri, s3_bucket_name, transcription_output_key)
    if job_name:
        print(f"Transcription job '{job_name}' started")
    else:
        print("Failed to start transcription job")
        return

    # Wait for the transcription job to complete
    while True:
        status = get_transcription_job_status(job_name)
        if status == 'COMPLETED':
            print("Transcription job completed")
            break
        elif status == 'FAILED':
            print("Transcription job failed")
            return
        else:
            print(f"Transcription job status: {status}")
            time.sleep(30)  # Wait for 30 seconds before checking again

    # Download the transcription file
    temp_transcription_path = f"{output_file_path}.json"
    if download_file(s3_bucket_name, transcription_output_key, temp_transcription_path):
        print(f"Successfully downloaded transcription to {temp_transcription_path}")
        
        # Extract raw transcript
        extract_raw_transcript(temp_transcription_path, output_file_path)
        os.remove(temp_transcription_path)
        print(f"Raw transcript saved to {output_file_path}")
    else:
        print("Failed to download transcription")

if __name__ == "__main__":
    main()
