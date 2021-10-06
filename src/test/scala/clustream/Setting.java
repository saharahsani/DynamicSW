package clustream;

public class Setting {
    static String snapsPath = "/sahar/snaps";
    static int k = 5;
    static int h = 1;
    static int numPoints = 5000;
    static Integer unitTimeDigit1 = 10000;
    static Integer unitTimeDigit2 = 40000;
    static Integer unitTimeDigit3 = 160000;
    static Integer unitTimeDigit4 = 320000;
    static Integer unitTimeDigit5 = 840000; //578000;
    static Integer unitTimeDigit6 = 1080000;
    static Integer unitTimeDigit7 = 2200000;
    static Integer unitTimeDigit8 = 3380000;
    // set init path
    static String initPathFile = "src/test/resources/initClusters/kdd/";
    static Boolean initialize=true;
    // for online phase
    static Integer windowTime =6;
    static String centersOnlinePath = "src/test/resources/res/centersCovSW-speed";
    static Integer centersStartNum =6;
    static Boolean rmExpirePoint=true;
    static Boolean expirePhase=false;
    static Integer runNum=4;
    static Integer numDimension =2;

    // for offline phase
   // static String centersOfflinePath = "src/test/resources/resFinal/fCentersPowDyBSKW4";
    static String centersOfflinePath = "src/test/resources/resFinal/fCentersCovSW-K15-sp";

    static Integer runNumber=4;
    static Integer horizon =1;
    static Integer numPoint = 5000;
    static Integer StartNum =4;
    static Integer EndNum =200;
    // kmeans = 0 , bisecting= 1 , mrg=2
    static Integer algoName=2;
}
