package com.pipeline.app;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;


@DefaultSchema(JavaFieldSchema.class)
class UserData {
    int user_id;
    String user_name;
    long user_phno;
    User_Address user_address;
}