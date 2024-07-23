package com.pipeline.app;

import java.util.Date;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;


@DefaultSchema(JavaFieldSchema.class)
class Sales {
    Date Date;
    int user_id;
    String Product_Name;
    int Quantity;
    String Unit_Price;
    String Total_Sale_Amount;
  }