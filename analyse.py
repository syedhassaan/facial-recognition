import json
import mysql.connector
import boto3
import time


def lambda_handler(event, context):
    # Get the bucket and key from the event
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_key = event["Records"][0]["s3"]["object"]["key"]

    # Create a Rekognition client
    rekognition_client = boto3.client("rekognition")

    # Set the desired collection ID
    collection_id = "surveystance"

    # Delete the collection
    # delete_collection(collection_id, rekognition_client)

    # Delete certain faces
    # delete_faces(collection_id, rekognition_client, face_ids)

    # Call the function to create the collection
    create_collection(collection_id, rekognition_client)

    index_faces(
        collection_id,
        rekognition_client,
        bucket_name,
        file_key,
        event["Records"][0]["awsRegion"],
    )

    return {"statusCode": 200, "body": json.dumps("Photo analysed successfully!")}


def delete_collection(collection_id, rekognition_client):
    response = rekognition_client.delete_collection(CollectionId=collection_id)

    # Check the response status
    if response["StatusCode"] == 200:
        print(f"Collection '{collection_id}' deleted successfully.")
    else:
        print(
            f"Failed to delete collection '{collection_id}'. Error message: {response['Message']}"
        )


def delete_faces(collection_id, rekognition_client, face_ids):
    faces_deleted = rekognition_client.delete_faces(
        CollectionId=collection_id, FaceIds=face_ids
    )
    print("faces_deleted: ", faces_deleted)


def create_collection(collection_id, rekognition_client):
    # List existing collections
    collections = rekognition_client.list_collections()
    print("Collections: ", collections)

    if collection_id in collections["CollectionIds"]:
        print("Collection already exists")

    else:
        # Create the collection
        response = rekognition_client.create_collection(CollectionId=collection_id)

        # Retrieve the collection ARN
        collection_arn = response["CollectionArn"]
        print("Collection created: ", collection_arn)


def index_faces(collection_id, rekognition_client, bucket_name, file_key, region):
    # Index faces in the image
    response = rekognition_client.index_faces(
        CollectionId=collection_id,
        Image={"S3Object": {"Bucket": bucket_name, "Name": file_key}},
        DetectionAttributes=["ALL"],
    )

    print("index_faces response: ", response)

    image_url = (
        "https://" + bucket_name + ".s3." + region + ".amazonaws.com/" + file_key
    )
    print("image_url: ", image_url)

    i = 0

    if len(response["FaceRecords"]) > 0:
        for face_records in response["FaceRecords"]:
            print("Face #" + str(i) + ": ", face_records)

            face_id = face_records["Face"]["FaceId"]
            print("face_id: ", face_id)

            emotion = face_records["FaceDetail"]["Emotions"][0]["Type"]
            print("emotion: ", emotion)

            age_range_low = face_records["FaceDetail"]["AgeRange"]["Low"]
            print("age_range_low: ", age_range_low)

            age_range_high = face_records["FaceDetail"]["AgeRange"]["High"]
            print("age_range_high: ", age_range_high)

            gender = face_records["FaceDetail"]["Gender"]["Value"]
            print("gender: ", gender)

            data = str(face_records["FaceDetail"])
            print("data: ", data)

            similar_faces = get_similar_faces(
                face_id, collection_id, rekognition_client
            )

            db_insert(
                emotion,
                age_range_low,
                age_range_high,
                gender,
                data,
                image_url,
                file_key,
                face_id,
                similar_faces,
            )

            i += 1
    else:
        print("No face detected in image provided")


def get_similar_faces(face_id, collection_id, rekognition_client):
    retries = 0
    while retries < 5:
        try:
            print("face_id received: ", face_id)
            similar_faces = rekognition_client.search_faces(
                CollectionId=collection_id, FaceId=face_id
            )

            print("similar_faces: ", similar_faces)

            break

        except Exception as E:
            print("Exception::", E)
            print("Retrying search for similar_face_ids. Attempt: ", retries)
            retries += 1
            time.sleep(3)
            if retries == 5:
                print("Maximum retries attempted. Exiting function now")
                return

    all_similar_face_ids = {}
    # Process the response
    if "FaceMatches" in similar_faces:
        face_matches = similar_faces["FaceMatches"]
        print("Matches found: ", len(face_matches))

        if len(face_matches) > 0:
            for match in face_matches:
                similarity = match["Similarity"]
                matched_face_id = match["Face"]["FaceId"]
                print("similarity: ", similarity)
                print("matched_face_id: ", matched_face_id)
                all_similar_face_ids[matched_face_id] = similarity

            return json.dumps(all_similar_face_ids)

        else:
            print("No matches faces found")

    else:
        print("No matches found")


def db_insert(
    emotion,
    age_range_low,
    age_range_high,
    gender,
    response,
    image_url,
    image_name,
    face_id,
    similar_faces="{}",
):
    # Connect to the MySQL database
    conn = mysql.connector.connect(
        user="admin",
        password="",
        port=3306,
        host="database-1.xyz.us-west-1.rds.amazonaws.com",
        database="analytics",
    )
    print("Connection established with database")

    # Prepare a cursor object to execute SQL queries
    cursor = conn.cursor()

    # Define the SQL query to insert a record into the table
    query = (
        "INSERT INTO results "
        "(emotion, age_range_low, age_range_high, gender, response, image_url, image_name, face_id, similar_face_ids) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    # Define the values to insert into the table
    record_values = (
        emotion,
        age_range_low,
        age_range_high,
        gender,
        response,
        image_url,
        image_name,
        face_id,
        similar_faces,
    )
    print("Query prepared")

    # Execute the SQL query to insert the record
    cursor.execute(query, record_values)
    print("Query executed")

    # Commit the changes to the database
    conn.commit()

    # Close the database connection
    cursor.close()
    conn.close()
