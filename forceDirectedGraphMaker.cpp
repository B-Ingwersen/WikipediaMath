
// forceDirectedGraphMaker.cpp
// Create a force directed graph given a folder containing data defining the
// connections between a set of points; another file will be created with the
// locations of the points in the graph. The binary data format produced and
// read is compatible with what the 'buildNetworkGraphData' function in
// WikiDataPuller.py; it is recommended that this program is only invoked from
// that function instead of being used as a standalone program
//
// Author: Bryan Ingwersen
// Date: March 29, 2020

#include <cmath>
#include <cstring>
#include <iostream>

// check for windows file system
#ifdef _WIN32
    #define PATH_SEPARATOR "\\"
#else
    #define PATH_SEPARATOR "/"
#endif

// data structures for storing coordinates and related info
struct Point {
    double x;
    double y;
};
struct CenterOfMass {
    double x;
    double y;
    double mass;
};
struct ForceVector {
    double x;
    double y;
};
// boxes used to form the tree for a Barnes–Hut simulation
struct DiagramBox {
    // box which this box is contained in
    DiagramBox * parent;

    // the bounding coordinates of the box
    double xmin;
    double ymin;
    double xmax;
    double ymax;
    double boxSize;

    CenterOfMass cMass;

    // a box either has sub boxes sotred in 'subBoxes'
    // or it contains a point whose index is 'num'
    bool hasSubBoxes;
    DiagramBox * subBoxes[4];
    int num;

    // box methods
    void init(double xmin_, double ymin_, double xmax_, double ymax_,
        int nNums, unsigned int * nums, DiagramBox * parent_);
    void buildSubBoxes(int nNums, unsigned int * nums);
    CenterOfMass getMass();
    void deleteSubTree();
    ForceVector getRepulsionForce(int num_, Point pt);
};

// Function prototypes
void writePoints();
void initGraph();
void movePoints();
void iterate();
void loadData();
void writePoints();

// parameters on how the simulation works
double REPL_SCALE; // scales the repulsion force between points
double ATTR_SCALE; // scales the attraction force of links
double motionLimit; // limits how far a point can move in 1 iteration
double theta; // parameter of the Barnes–Hut simulation; smaller values
              // yield a more accurate simulation but takes longer

// point data used by simulation
int nPoints; // total number of points in the graph
Point * points; // coordinates of points
Point * pointsNew; // stores coordinates after an iteration
unsigned int ** connections; // store inter-article connections
unsigned int * numsList; // list of point indexes
DiagramBox topBox; // top level box of the Barnes–Hut simulation

// file names of input data files and output file
std::string dataFileName, offsetFileName, writeFileName;

// setup and run the simulation
int main(int argc, char ** argv) {
    // check that subindex data was passed
    if (argc != 2) {
        std::cout << "Usage: ./forceDirectedGraphMaker path/to/subindex" << std::endl;
        return 1;
    }

    // default simulation parameters
    REPL_SCALE = 0.02;
    ATTR_SCALE = 0.04;
    motionLimit = 0.2;
    theta = 0.5;

    // construct file names
    dataFileName = (std::string)argv[1] + PATH_SEPARATOR + "linkData.bin";
    offsetFileName = (std::string)argv[1] + PATH_SEPARATOR + "linkOffsets.bin";
    writeFileName = (std::string)argv[1] + PATH_SEPARATOR + "networkGraphData.bin";

    srand(time(NULL));
    loadData(); // load the points and their connections
    initGraph(); // set the initial point locations

    // run simulation iterations
    for (int round = 0; round < 20; ++round) {
        for (int i = 0; i < 200; ++i) {
            iterate();
        }
        REPL_SCALE *= 0.8;
        ATTR_SCALE *= 0.8;
    }

    writePoints(); // write the results to disk

    return 0;
}

// create a box used in the Barnes–Hut simulation
void DiagramBox::init(double xmin, double ymin, double xmax, double ymax,
    int nNums, unsigned int * nums, DiagramBox * parent) {
    this -> xmin = xmin;
    this -> ymin = ymin;
    this -> xmax = xmax;
    this -> ymax = ymax;
    this -> boxSize = xmax - xmin;
    this -> parent = parent;

    // check if there are one or none points in the box; otherwise, split the
    // box into further boxes recursively until that condition is true
    if (nNums == 0) {
        this -> hasSubBoxes = false;
        this -> num = -1;
    }
    else if (nNums == 1) {
        this -> hasSubBoxes = false;
        this -> num = nums[0];
    }
    else {
        this -> hasSubBoxes = true;
        this -> buildSubBoxes(nNums, nums);
        this -> num = -1;
    }
}

// construct four sub-boxes within a box
// nums is an array of point indexes, nNums is the array length
void DiagramBox::buildSubBoxes(int nNums, unsigned int * nums) {
    int nBLNums = 0; // bottom left nums
    int nBRNums = 0; // bottom right
    int nTLNums = 0; // top left
    int nTRNums = 0; // top right

    // split the box into four equally sized quarters
    double xmid = (this -> xmin + this -> xmax) / 2;
    double ymid = (this -> ymin + this -> ymax) / 2;

    // calculate the number of entries for each subbox to have
    int i;
    for (i = 0; i < nNums; i++) {
        unsigned int n = nums[i];
        double x = points[n].x;
        double y = points[n].y;
        if (x > xmid) {
            if (y > ymid) {
                nTRNums++;
            }
            else {
                nBRNums++;
            }
        }
        else {
            if (y > ymid) {
                nTLNums++;
            }
            else {
                nBLNums++;
            }
        }
    }

    // create new arrays to hold the numbers
    unsigned int * blNums, * brNums, * tlNums, * trNums;
    blNums = new unsigned int[nBLNums];
    brNums = new unsigned int[nBRNums];
    tlNums = new unsigned int[nTLNums];
    trNums = new unsigned int[nTRNums];

    // distribute the point indexes into their respective boxes
    int blPtr = 0;
    int brPtr = 0;
    int tlPtr = 0;
    int trPtr = 0;
    for (i = 0; i < nNums; i++) {
        unsigned int n = nums[i];
        double x = points[n].x;
        double y = points[n].y;
        if (x > xmid) {
            if (y > ymid) {
                trNums[trPtr] = nums[i];
                trPtr++;
            }
            else {
                brNums[brPtr] = nums[i];
                brPtr++;
            }
        }
        else {
            if (y > ymid) {
                tlNums[tlPtr] = nums[i];
                tlPtr++;
            }
            else {
                blNums[blPtr] = nums[i];
                blPtr++;
            }
        }
    }

    // create the subboxes
    this -> subBoxes[0] = new DiagramBox;
    this -> subBoxes[1] = new DiagramBox;
    this -> subBoxes[2] = new DiagramBox;
    this -> subBoxes[3] = new DiagramBox;
    this -> subBoxes[0] -> init(this -> xmin, this -> ymin, xmid, ymid, nBLNums, blNums, this);
    this -> subBoxes[1] -> init(xmid, this -> ymin, this -> xmax, ymid, nBRNums, brNums, this);
    this -> subBoxes[2] -> init(this -> xmin, ymid, xmid, this -> ymax, nTLNums, tlNums, this);
    this -> subBoxes[3] -> init(xmid, ymid, this -> xmax, this -> ymax, nTRNums, trNums, this);

    // free the arrays of indexes
    delete blNums;
    delete brNums;
    delete tlNums;
    delete trNums;
}

// find a diagram box's center of mass
CenterOfMass DiagramBox::getMass() {
    double x = 0;
    double y = 0;
    double mass = 0;
    if (this -> hasSubBoxes) {
        // take a weighted average of the subboxes center of masses
        int i;
        for (i = 0; i < 4; i++) {
            CenterOfMass subMass = this -> subBoxes[i] -> getMass();
            x += subMass.x * subMass.mass;
            y += subMass.y * subMass.mass;
            mass += subMass.mass;
        }
        if (mass > 0) {
            x /= mass;
            y /= mass;
        }
    }
    else if (this -> num != -1) {
        // return the single points location and mass
        x = points[this -> num].x;
        y = points[this -> num].y;
        mass = 1;
    }

    cMass = {x, y, mass};
    return {x, y, mass};
}

// recursively free all the sub-boxes within a box
void DiagramBox::deleteSubTree() {
    if (this -> hasSubBoxes) {
        int i;
        for (i = 0; i < 4; i++) {
            this -> subBoxes[i] -> deleteSubTree();
            delete this -> subBoxes[i];
        }
    }
}

// calculate the repulsion force that the elements in a box exert on a point
ForceVector DiagramBox::getRepulsionForce(int num, Point pt) {
    // ignore repulsion of a point with itself
    if (num == this -> num) {
        return {0,0};
    }

    // repulsion from a single point contained within a box
    if (this -> num != -1) {
        double X = points[this -> num].x;
        double Y = points[this -> num].y;

        double d = hypot(X - pt.x, Y - pt.y);
        double forceX = (pt.x - X) / (d * d * d);
        double forceY = (pt.y - Y) / (d * d * d);

        return {forceX, forceY};
    }
    // check if the box is empty
    if (!(this -> hasSubBoxes)) {
        return {0,0};
    }

    // if the ratio of the box's size to its distance from the point is small
    // enough, approximate as if all the box's mass was located at its center of
    // mass (this is the Barnes-Hut simulation technique for reducing the time
    // complexity from O(n^2) to O(n log(n))
    double d = hypot(pt.x - this -> cMass.x, pt.y - this -> cMass.y);
    if (this -> boxSize < theta * d) {
        double X = this -> cMass.x;
        double Y = this -> cMass.y;

        double forceX = this -> cMass.mass * (pt.x - X) / (d * d * d);
        double forceY = this -> cMass.mass * (pt.y - Y) / (d * d * d);

        return {forceX, forceY};
    }

    // add the repulsion forces from each sub-box
    double forceX = 0;
    double forceY = 0;
    for (int i = 0; i < 4; i++) {
        ForceVector v = this -> subBoxes[i] -> getRepulsionForce(num, pt);
        forceX += v.x;
        forceY += v.y;
    }
    return {forceX, forceY};
}

// seed the simulation with random starting positions for every node
void initGraph() {
    // allocate new arrays
    points = new Point[nPoints];
    pointsNew = new Point[nPoints];
    numsList = new unsigned int[nPoints];

    // set a random starting position for each point
    int i;
    for (i = 0; i < nPoints; i++) {
        points[i].x = (double)rand() / RAND_MAX;
        points[i].y = (double)rand() / RAND_MAX;

        numsList[i] = i;
    }
}

// apply one step of the attraction and repulsion forces
void movePoints() {
    // keep the points restrained to a 20x20 square
    double xmin = -10.0;
    double xmax = 10.0;
    double ymin = -10.0;
    double ymax = 10.0;

    // initialize the box structure for the Barnes–Hut simulation
    topBox.init(xmin, ymin, xmax, ymax, nPoints, numsList, NULL);
    topBox.getMass();

    // apply the repulsion force to each node
    for (int i = 0; i < nPoints; ++i) {
        ForceVector v = topBox.getRepulsionForce(i, points[i]);
        pointsNew[i].x += v.x * REPL_SCALE;
        pointsNew[i].y += v.y * REPL_SCALE;
    }
    
    // apply the attraction force to each node
    for (int i = 0; i < nPoints; i++) {

        double x = points[i].x;
        double y = points[i].y;
        double forceX = 0;
        double forceY = 0;
        
        // add the attraction force of each inter-node link
        unsigned int * cNums = connections[i];
        int j = 0;
        while (true) {
            unsigned int cNum = cNums[j];
            // 0xFFFFFFFF marks the last element connections array
            if (cNum == 0xFFFFFFFF) {
                break;
            }
            
            double X = points[cNum].x;
            double Y = points[cNum].y;

            // force is proportional to distance between nodes (Hooke's law)
            forceX += X - x;
            forceY += Y - y;
            
            // apply the equal & opposite force to the other node
            pointsNew[cNum].x -= (X - x) * ATTR_SCALE;
            pointsNew[cNum].y -= (Y - y) * ATTR_SCALE;

            j++;
        }
        // save the aggregate force
        pointsNew[i].x += forceX * ATTR_SCALE;
        pointsNew[i].y += forceY * ATTR_SCALE;
    }

    // clean up the data structures of the Barnes–Hut simulation
    topBox.deleteSubTree();
}

// calculate the graph state after an additional time step
void iterate() {
    // limit the simulation to a 20 x 20 unit square
    double xmin = -10.0;
    double xmax = 10.0;
    double ymin = -10.0;
    double ymax = 10.0;

    // copy the current state as the starting point for the new state
    memcpy(pointsNew, points, nPoints * sizeof(Point));

    // apply the attractive and 
    movePoints();

    // cap the maximum distance a node can travel and keep nodes within a radius
    // 10 circle around the origin
    double dTotal = 0;
    for (int i = 0; i < nPoints; i++) {
        double xold = points[i].x;
        double yold = points[i].y;
        double xnew = pointsNew[i].x;
        double ynew = pointsNew[i].y;

        // limit a node's motion within one time step (two nodes that end up
        // very close will apply an extreme repulsion force which might move
        // them dramatically away from their ideal position)
        double dx = xnew - xold;
        double dy = ynew - yold;
        double d = hypot(dx, dy);
        dTotal += d;
        if (d > motionLimit) {
            xnew = xold + dx / d * motionLimit;
            ynew = yold + dy / d * motionLimit;
            pointsNew[i].x = xnew;
            pointsNew[i].y = ynew;
        }

        // keep points within a radius 10 circle around the origin
        double l = hypot(xnew, ynew);
        if (l > 10) {
            pointsNew[i].x = xnew / l * 10;
            pointsNew[i].y = ynew / l * 10;
        }
    }

    // swap the old and new point arrays
    Point * temp = points;
    points = pointsNew;
    pointsNew = temp;
}

// read a little endian 32-bit integer from a file
uint32_t readUint32LittleEndian(FILE * file) {
    uint8_t byte;
    int32_t value = 0;
    for (int i = 0; i < 4; ++i) {
        fread(&byte, 1, 1, file);
        value <<= 8;
        value += (uint32_t)byte;
    }
    return value;
}

// write a little endian 64-bit integer to a file
void writeUint64LittleEndian(FILE * file, uint64_t value) {
    uint8_t byte;
    for (int i = 0; i < 8; ++i) {
        byte = (uint8_t)(value & 0xFF);
        value >>= 8;
        fwrite(&byte, 1, 1, file);
    }
}

// load the node-connection data from the disk
void loadData() {
    // data overview: dataFile is essentially a list of lists of 32-bit integers
    // corresponding to the indexes of the nodes which each node connects to;
    // these lists are terminated with the special entry 0xFFFFFFFF; offsetsFile
    // is a list of 32-bit integers corresponding to the offset in dataFile
    // where each node's list begins

    // open the data file and find its size
    FILE * dataFile = fopen(dataFileName.c_str(),  "r");
    fseek(dataFile, 0, SEEK_END);
    int dataSize = ftell(dataFile);

    // read the file into an array
    unsigned int * dataBuffer = new unsigned int[dataSize / 4];
    fseek(dataFile, 0, SEEK_SET);
    for (int i = 0; i < (dataSize / 4); ++i) {
        dataBuffer[i] = readUint32LittleEndian(dataFile);
    }
    fclose(dataFile);

    // open the offsets file and calculate the number of entries it contains
    FILE * offsetsFile = fopen(offsetFileName.c_str(), "r");
    fseek(offsetsFile, 0, SEEK_END);
    nPoints = ftell(offsetsFile) / 4;
    fseek(offsetsFile, 0, SEEK_SET);

    // read the offset file to find where each node's conection list begins
    connections = new unsigned int*[nPoints];
    for (int i = 0; i < nPoints; i++) {
        unsigned int offset = readUint32LittleEndian(offsetsFile);
        connections[i] = dataBuffer + offset / 4;
    }
    fclose(offsetsFile);
}

// write the point locaiton data to a file
void writePoints() {
    // file format: a list of 16 byte entries corresponding to each node; the
    // entry is composed of two 64 bit integers reprsenting the x and y coords
    // of the point; each coordinate is really a float between -10 and 10, but
    // they are linearly mapped to integers between 0 and 10^16

    std::cout << "Beginning to write to file: " << writeFileName << std::endl;

    // write each point to the file
    FILE * writeFile = fopen(writeFileName.c_str(), "w");
    for (int i = 0; i < nPoints; i++) {
        // linearly map the coords from [-10, 10] to [0, 10^16]
        double x = points[i].x + 10.0;
        double y = points[i].y + 10.0;
        if (x < 0) {
            x = 0;
        }
        if (y < 0) {
            y = 0;
        }
        long X = (long)(x * 1000000000000000.0); // 10^15
        long Y = (long)(y * 1000000000000000.0);

        writeUint64LittleEndian(writeFile, X);
        writeUint64LittleEndian(writeFile, Y);
    }

    fclose(writeFile);

    std::cout << "Done writing to file!" << std::endl;
}
