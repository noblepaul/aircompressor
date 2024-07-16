/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.compress.v2;

import io.airlift.compress.v2.lz4.Lz4JavaCompressor;
import io.airlift.compress.v2.lz4.Lz4JavaDecompressor;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Codec {
    static int BLOCK_SZ = 256*1024;//256KB

    //run this method to decompress
    public static void main(String[] args) throws Exception {
        List<Path> filePaths =  listFiles("/Users/noble/src/FS/aircompressor/data/lz4_idx");
        long start = System.currentTimeMillis();
        Decompressor dcomp = new Lz4JavaDecompressor();
        System.out.println("INFLATE starting : "+dcomp.toString());
        for (Path filePath : filePaths) {
            String s = filePath.getFileName().toString();
            decompressBlocks(filePath, Path.of("/Users/noble/src/FS/aircompressor/data/index", s.substring(0,s.length()-2)),  dcomp);
        }
        System.out.println("Total time taken to decompress: "+ (System.currentTimeMillis() - start));
    }

    //run this method to compress

    public static void main1(String[] args) throws IOException {

        Compressor comp=  new Lz4JavaCompressor();
        System.out.println("DEFLATE starting : "+comp.toString());

        long start = System.currentTimeMillis();
        List<Path> filePaths =  listFiles("/Users/noble/src/FS/aircompressor/data/index");
        int totalInputBytes = 0;
        int totalOut = 0;
        for (Path filePath : filePaths) {
            byte[] fileBytes = Files.readAllBytes(filePath);
            totalInputBytes+= fileBytes.length;
            totalOut+= compressBlocks(fileBytes, "/Users/noble/src/FS/aircompressor/data/lz4_idx/"+ filePath.getFileName().toString(), comp);
        }

        System.out.println("Total in: "+ totalInputBytes+" out : "+ totalOut +  " compression ratio " +((float)(totalOut*100/totalInputBytes))+ " time taken : "+ (System.currentTimeMillis() - start));
    }




    //compress a file in BLOCKS. All data is written to the same file with a small delimiter which denotes the size. -1 is EOF
    private static int compressBlocks(byte[] fileBytes, String file, Compressor comp) throws IOException {
        int totalWritten = 0;
        int start = 0;
        byte[] compressedBytes = new byte[comp.maxCompressedLength(BLOCK_SZ*2)];
        try (DataOutputStream daos = new DataOutputStream(new FileOutputStream(file + ".z"))) {
            boolean done = false;
            for (;; ) {
                int sz = 0;
                //we need to write in blocks
                if (fileBytes.length > start + BLOCK_SZ) {
                    sz = comp.compress(fileBytes, start, BLOCK_SZ, compressedBytes, 0, compressedBytes.length);
                    start += BLOCK_SZ;
                } else {
                    // last BLOCK
                    sz = comp.compress(fileBytes, start, fileBytes.length - start, compressedBytes, 0, compressedBytes.length);
                    start = fileBytes.length;
                    done = true;
                }
                daos.writeInt(sz);
                daos.write(compressedBytes, 0, sz);
                totalWritten += 4;
                totalWritten += sz;
                if(done) {
                    totalWritten+=4;
                    daos.writeInt(-1);
                    daos.flush();
                    break;
                }
            }
        }
        return totalWritten;

    }

    // use this method to compress without blocks
    private static int compress(byte[] fileBytes, String file, Compressor comp) throws IOException {
        byte[] compressedBytes = new byte[comp.maxCompressedLength(fileBytes.length)];
        int sz = comp.compress(fileBytes, 0, fileBytes.length, compressedBytes, 0, compressedBytes.length);
        try(OutputStream o = new FileOutputStream(file +".z")) {
            o.write(compressedBytes, 0, sz);
        }
        return sz;
    }

    private static int decompressBlocks(Path filePath, Path original, Decompressor dcomp) throws IOException {
        byte[] fileBytes = Files.readAllBytes(filePath);
        System.out.println(filePath.getFileName() + " "+ fileBytes.length);

        byte[] bytes =   new byte[(int)Files.size(original)];
        byte[] bytesScratch =   new byte[(int)Files.size(original)];


        try(DataInputStream in = new DataInputStream(new ByteArrayInputStream(fileBytes))){
            for(;;){
                int sz = in.readInt();
                if(sz <0) break;
                in.read(bytes, 0, sz);
                dcomp.decompress(bytes,0,sz, bytesScratch, 0, bytesScratch.length);
                if(sz< BLOCK_SZ) break;
            }
        }
        return bytes.length;
    }


    private static List<Path> listFiles(String directoryPath) throws IOException {
        try (Stream<Path> paths = Files.walk(Paths.get(directoryPath))) {
            return paths.filter(Files::isRegularFile).collect(Collectors.toList());
        }
    }

}
