package io.hgis.rasterize;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

public class RasterWriter {

    public static void paint(int width, int height, int[] vals, File file) throws IOException {
//        for (int z = 0; z < height; z++) {
//            if (vals[z] > 0)
//            System.out.println("yes");
//        }

        int[] data = new int[width * height];
        int i = 0;
        for (int y = 0; y < height; y++) {
//            int red = (y * 255) / (height - 1);
            for (int x = 0; x < width; x++) {
                int red = vals[i];
                int green = 0;
                int blue = 0;
                data[i] = (red << 16) | (green << 8) | blue;
                i++;
            }
        }
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        image.setRGB(0, 0, width, height, data, 0, width);
        FileOutputStream fos = new FileOutputStream(file);
        ImageIO.write(image, "png", fos);
    }

    public static void main(String[] args) throws IOException {
//        paint(300, 300);
    }
}