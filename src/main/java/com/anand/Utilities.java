package com.anand;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Created by anand.ranganathan on 9/20/15.
 */
public class Utilities {




    // These are temporary directory created in the beginning and deleted after the execution finished.

    public static File getDirectory(String dirType) {

        String tempFileName = System.getProperty("java.io.tmpdir");


        File f = new File(tempFileName);

        String  folder = System.getProperty("java.io.tmpdir")+dirType+"-"+ new java.util.Date().getTime();
        File file = new File(folder);
        //   file.deleteOnExit();
        return file;
    }



    public static void deleteDirectory(File directory) throws FileNotFoundException {
        if(!directory.exists()){
            throw new FileNotFoundException(directory.getAbsolutePath());
        }

        if(directory.isDirectory()){
            for(File f: directory.listFiles()){
                deleteFiles(f);
            }
        }

        directory.delete();
    }

    public static void deleteFiles(File file) {
        if (file.exists()) {
            file.delete();
        }
    }
}
