package mongothread;

/**
 *
 * @author FarmerLuo
 * @version 0.4
 * 
 * changelogs:
 *
 * 2010-04-21:
 * 1)把mongo-1.3.jar更换为mongo-1.4.jar，解决了大量插入或更新有时报错的问题
 * 
 * 2010-04-15:
 * 1) mysql并发测试
 * 2）mongodb并发测试
 *
 * Last Date: 2010.04.21
 *
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        long rows = 0;
        long start = System.currentTimeMillis();

        if ( args.length < 6 ) {
            usage(1);
        }

        if ( args[0].equals("mysql") && args.length < 7 ) {
            usage(2);
        }

        rows = Long.parseLong(args[2]);
        int tnum = 0;

        if ( args[0].equals("mysql") ) {
            tnum = Integer.parseInt(args[3]);
            mysqldb[] mysqldbthread = new mysqldb[tnum];
            for ( int k = 0; k < tnum; k++ ) {
                mysqldbthread[k] = new mysqldb(args[1],rows,args[4],args[5],args[6],args[7],k);
                mysqldbthread[k].start();
            }
            for ( int k = 0; k < tnum; k++ ) {
//                System.out.println("mongothread["+k+"].isAlive()=" + mongothread[k].isAlive());
                if ( mysqldbthread[k].isAlive() ) {
                    try {
                        mysqldbthread[k].join();
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
//                        System.out.println(ex.getMessage());
//                        System.out.println(ex.toString());
                    }
                }
            }
        } else if ( args[0].equals("mongo") ) {
            tnum = Integer.parseInt(args[3]);
            mongodb[] mongothread = new mongodb[tnum];
            for ( int k = 0; k < tnum; k++ ) {
                mongothread[k] = new mongodb(args[1],rows,args[4],args[5],k);
                mongothread[k].start();
            }
            for ( int k = 0; k < tnum; k++ ) {
//                System.out.println("mongothread["+k+"].isAlive()=" + mongothread[k].isAlive());
                if ( mongothread[k].isAlive() ) {
                    try {
                        mongothread[k].join();
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
//                        System.out.println(ex.getMessage());
//                        System.out.println(ex.toString());
                    }
                }
            }
        } else {
            usage(3);
        }

        long stop = System.currentTimeMillis();
        long endtime = (stop - start)/1000;
        if ( endtime == 0 ) endtime = 1;
        long result = rows/endtime;
        long tresult = rows*tnum/endtime;

        System.out.print("Total thread:" + tnum + "\n");
        System.out.print("Total run time:" + endtime + " sec\n");
        System.out.print("Per-thread rows:" + rows + "\n");
        System.out.print("Per-thread " + args[0] + " " + args[1] + " Result:" + result + "row/sec\n");
        System.out.print("Total rows:" + rows * tnum + "\n");
        System.out.print("Total " + args[0] + " " + args[1] + " Result:" + tresult + "row/sec\n");
    }

    public static void usage(int errorno){
        System.out.print("Usage:\n");
        System.out.print("mysql test:\n");
        System.out.print("java -jar mongotest.jar < mysql > < [select | update | insert] > < rows > <concurrent> < host > < username > < password>  <database> \n");
        System.out.print("mongo test:\n");
        System.out.print("java -jar mongotest.jar < mongo > < [select | update | insert] > < rows > <concurrent> < host > <database> \n");
        System.exit( errorno );
    }

}
