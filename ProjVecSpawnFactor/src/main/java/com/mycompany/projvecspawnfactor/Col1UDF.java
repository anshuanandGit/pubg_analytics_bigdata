/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.projvecspawnfactor;

import org.apache.spark.sql.api.java.UDF2;

/**
 *
 * @author Anshu Anand
 */
public class Col1UDF implements UDF2<String, String,String> {

    @Override
    public String call(String ride, String walk) throws Exception {
       String vsf = "";
        double vsfd = 0.0;

        double r = Double.parseDouble(ride);
        double w = Double.parseDouble(walk);
        if (r > 0.0 && w > 0.0) {

            vsfd = w / r;
            vsf = Double.toString(vsfd);

        } else {
            vsf = "NA";
        }
        return vsf;
    }

    

}
