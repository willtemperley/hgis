package io.hgis.rasr;



public class Rasterizer {

    /**
     *
     * http://www.cs.helsinki.fi/group/goa/mallinnus/lines/bresenh.html
     *
     * @param x1 start x
     * @param y1 start y
     * @param x2 end x
     * @param y2 end y
     * @param plotter plots x and y values. Colours can be handled if the plotter is used as a state machine.
     */
    public static void rasterize(int x1, int y1, int x2, int y2, Plotter plotter) {

        // First check whether we're iterating in x or y direction
        int dx = Math.abs(x2 - x1);
        int dy = Math.abs(y2 - y1);


        if (dy < dx) {
            // X changes faster than Y
            if (x2 < x1) {
                // If drawing right to left, swapping endpoints makes it the same problem as left to right.
                // This means only two octants now need to be considered when X changes faster than Y
                rasterize(x2, y2, x1, y1, plotter);
                return;
            }
            if (y1 < y2) {
                rasterizePositiveX(x1, y1, x2, y2, plotter);
            } else {
                rasterizeNegativeX(x1, y1, x2, y2, plotter);
            }
        } else {
            // Y changes faster than X
            // Same deal when iterating Y coords - make sure y2 is less than y1, only need to think of two octants
            if (y2 < y1) {
                //Swap coords
                rasterize(x2, y2, x1, y1, plotter);
                return;
            }
            if (x1 < x2) {
                rasterizePositiveY(x1, y1, x2, y2, plotter);
            } else {
                rasterizeNegativeY(x1, y1, x2, y2, plotter);
            }
        }

    }

    private static void rasterizePositiveX(int x1, int y1, int x2, int y2, Plotter view) {
        int dx = x2 - x1;
        int dy = y2 - y1;
        int y = y1;
        int eps = 0;

        for (int x = x1; x <= x2; x++) {
            view.plot(x, y);
            eps += dy;
            if ((eps << 1) >= dx) {
                y++;
                eps -= dx;
            }
        }
    }

    private static void rasterizePositiveY(int x1, int y1, int x2, int y2, Plotter view) {
        int dx = x2 - x1;
        int dy = y2 - y1;
        int x = x1;
        int eps = 0;

        for (int y = y1; y <= y2; y++) {
            view.plot(x, y);
            eps += dx;
            if ((eps << 1) >= dy) {
                x++;
                eps -= dy;
            }
        }
    }

    private static void rasterizeNegativeX(int x1, int y1, int x2, int y2, Plotter view) {
        int dx = x2 - x1;
        int dy = y2 - y1;
        int y = y1;
        int eps = 0;

        for (int x = x1; x <= x2; x++) {
            view.plot(x, y);
            eps += dy;
            if ((eps << 1) < dx) {
                y--;
                eps += dx;
            }
        }
    }

    private static void rasterizeNegativeY(int x1, int y1, int x2, int y2, Plotter view) {

        int dx = x2 - x1;
        int dy = y2 - y1;
        int x = x1;
        int eps = 0;

        for (int y = y1; y <= y2; y++) {
            view.plot(x, y);
            eps += dx;
            if ((eps << 1) < dy) {
                x--;
                eps += dy;
            }
        }
    }



}