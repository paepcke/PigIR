/*
 *
 *
 *	Simple Good-Turing Frequency Estimator
 *
 *
 *	Geoffrey Sampson, with help from Miles Dennis
 *
 *	Department of Informatics
 *	Sussex University
 *
 *	www.grsampson.net
 *
 *
 *	First release:  27 June 1995
 *	Revised release:  24 July 2000
 *	This header information revised:  23 March 2005
 *	Further revised release:  8 April 2008
 *	Further revised release:  11 July 2008
 *
 *
 *	Takes a set of (frequency, frequency-of-frequency) pairs, and 
 *	applies the "Simple Good-Turing" technique for estimating 
 *	the probabilities corresponding to the observed frequencies, 
 *	and P.0, the joint probability of all unobserved species.
 *	The Simple Good-Turing technique was devised by the late William
 *	A. Gale of AT&T Bell Labs, and described in Gale & Sampson, 
 *	"Good-Turing Frequency Estimation Without Tears" (JOURNAL
 *	OF QUANTITATIVE LINGUISTICS, vol. 2, pp. 217-37 -- reprinted in
 *	Geoffrey Sampson, EMPIRICAL LINGUISTICS, Continuum, 2001).
 *
 *	Anyone is welcome to take copies of this program and use it
 *	for any purpose, at his or her own risk.  If it is used in
 *	connexion with published work, acknowledgment of Sampson and
 *	the University of Sussex would be a welcome courtesy.
 *
 *	The program is written to take input from "stdin" and send output
 *	to "stdout"; redirection can be used to take input from and
 *	send output to permanent files.  The code is in ANSI standard C.
 *
 *	The input file should be a series of lines separated by newline
 *	characters, where all nonblank lines contain two positive integers
 *	(an observed frequency, followed by the frequency of that frequency)
 *	separated by whitespace or comma [Andreas].  (Blank lines are ignored.)
 *	The lines should be in ascending order of frequency, and must
 *	begin with frequency 1.
 *
 *	No checks are made for linearity; the program simply assumes that the 
 *	requirements for using the SGT estimator are met.
 *
 *	The output is a series of lines each containing an integer followed  
 *	by a probability (a real number between zero and one), separated by a 
 *	tab.  In the first line, the integer is 0 and the real number is the 
 *	estimate for P.0.  In subsequent lines, the integers are the  
 *	successive observed frequencies, and the reals are the estimated  
 *	probabilities corresponding to those frequencies.
 *
 *	Later releases cure bugs to which my attention has kindly been
 *	drawn at different times by Martin Jansche of Ohio State University
 *	and Steve Arons of New York City.  No warranty is given 
 *	as to absence of further bugs.
 *
 *	Fan Yang of Next IT Inc., Spokane, Washington, has suggested to me 
 *      that in the light of his experience with the SGT technique, for some 
 *	data-sets it could be preferable to use the 0.1 significance criterion
 *	actually used in the experiments reported in the Gale & Sampson
 *	paper, rather than the 0.05 criterion suggested in that paper
 *	for the sake of conformity with standard statistical convention.
 *	(See note 8 of the paper.)  Neither Fan Yang nor I have pursued
 *	this far enough to formulate a definite recommendation; but, in
 *	order to make it easier for users of the software to experiment
 *	with alternative confidence levels, the July 2008 release moves
 *	the relevant "magic number" out of the middle of the program into
 *	a #define line near the beginning where it is given the constant
 *	name CONFID_FACTOR.  The value 1.96 corresponds to the p < 0.05
 *	criterion; in order to use the p < 0.1 criterion, 1.96 in the 
 *	#define line should be changed to 1.65.
 *
 *
 * Andreas: 
 *     o changed maxrows from 200 to 20,000 
 *     o allowed comma as field separator on input
 *     o changed output separator to comma
 *
 *     Compilation: gcc goodTuringSmoothing.c -o goodTuringSmoothing -lm
 */
 
 
 #include <stdio.h>
 #include <math.h>
 #include <ctype.h>
 #include <stdlib.h>
 #include <string.h>
 
 #define TRUE   1
 #define FALSE  0
 #define MAX_LINE       100
 #define MAX_ROWS       20000
 #define MIN_INPUT      5
 #define CONFID_FACTOR	1.96
 
 int r[MAX_ROWS], n[MAX_ROWS];
 double Z[MAX_ROWS], log_r[MAX_ROWS], log_Z[MAX_ROWS], 
                rStar[MAX_ROWS], p[MAX_ROWS];
 int rows, bigN;
 double PZero, bigNprime, slope, intercept;
 
 int main(void)
        {
        int readValidInput(void);
        void analyseInput(void);
        
        if ((rows = readValidInput()) >= 0)
                {
                if (rows < MIN_INPUT)
                        printf("\nFewer than %d input value-pairs\n",
                                        MIN_INPUT);
                else
                        analyseInput();
                }
        return(TRUE);
        }
 
 double sq(double x)
        {
        return(x * x);
        }
 
 int readValidInput(void)
        /*
         *      returns number of rows if input file is valid, else -1
         *      NB:  number of rows is one more than index of last row
         *
         */
        
        {
        char line[MAX_LINE];
        const char* whiteSpace = " \t\n\v\f\r,";
        int lineNumber = 0;
        int rowNumber = 0;
        const int error = -1;

        while (fgets(line, MAX_LINE, stdin) != NULL && rowNumber < MAX_ROWS)
                {
                char* ptr = line;
                char* integer;
                int i;

                ++lineNumber;

                while (isspace(*ptr)) 
                        ++ptr;  /* skip white space at the start of a line */
                if (*ptr == '\0')
                        continue;
                if ((integer = strtok(ptr, whiteSpace)) == NULL || 
                                (i = atoi(integer)) < 1)
                        {
                        fprintf(stderr, "Invalid field 1, line %d\n",
                                        lineNumber);
                        return(error);
                        }
                if (rowNumber > 0 && i <= r[rowNumber - 1])
                        {
                        fprintf(stderr, 
                      "Frequency not in ascending order, line %d\n", 
                                        lineNumber);
                        return(error);
                        }
                r[rowNumber] = i;
                if ((integer = strtok(NULL, whiteSpace)) == NULL || 
                                (i = atoi(integer)) < 1)
                        {
                        fprintf(stderr, "Invalid field 2, line %d\n",
                                        lineNumber);
                        return(error);
                        }
                n[rowNumber] = i;
                if (strtok(NULL, whiteSpace) != NULL)
                        {
                        fprintf(stderr, "Invalid extra field, line %d\n",
                                        lineNumber);
                        return(error);
                        }
                ++rowNumber;
                }
        if (rowNumber >= MAX_ROWS)
                {
                fprintf(stderr, "\nInsufficient memory reserved for input\
                        values\nYou need to change the definition of\
                        MAX_ROWS\n");
                return(error);
                }
        return(rowNumber);
        }
         
 void findBestFit(void)
        {
        double XYs, Xsquares, meanX, meanY;
        double sq(double);
        int i;
        
        XYs = Xsquares = meanX = meanY = 0.0;
        for (i = 0; i < rows; ++i)
                {
                meanX += log_r[i];
                meanY += log_Z[i];
                }
        meanX /= rows;
        meanY /= rows;
        for (i = 0; i < rows; ++i)
                {
                XYs += (log_r[i] - meanX) * (log_Z[i] - meanY);
                Xsquares += sq(log_r[i] - meanX);
                }
        slope = XYs / Xsquares;
        intercept = meanY - slope * meanX;
        }
        
 double smoothed(int i)
        {
        return(exp(intercept + slope * log(i)));
        }
        
 int row(int i)
        {
        int j = 0;
        
        while (j < rows && r[j] < i)
                ++j;
        return((j < rows && r[j] == i) ? j : -1);
        }
        
 void showEstimates(void)
        {
        int i;
        
        // ANDREAS printf("0\t%.4g\n", PZero);
	printf("0,%.4g\n", PZero);
        for (i = 0; i < rows; ++i)
	  //ANDREAS printf("%d\t%.4g\n", r[i], p[i]);
	  printf("%d,%.4g\n", r[i], p[i]);
        }
        
 void analyseInput(void)
        {
        int i, j, next_n;
        double k, x, y;
        int indiffValsSeen = FALSE;
        int row(int);
        void findBestFit(void);
        double smoothed(int);
        double sq(double);
        void showEstimates(void);
        
        bigN = 0;
        for (j = 0; j < rows; ++j)
                bigN += r[j] * n[j];
        next_n = row(1);
        PZero = (next_n < 0) ? 0 : n[next_n] / (double) bigN;
        for (j = 0; j < rows; ++j)
                {
                i = (j == 0 ? 0 : r[j - 1]);
                if (j == rows - 1)
                        k = (double) (2 * r[j] - i);
                else
                        k = (double) r[j + 1];
                Z[j] = 2 * n[j] / (k - i);
                log_r[j] = log(r[j]);
                log_Z[j] = log(Z[j]);
                }
        findBestFit();
        for (j = 0; j < rows; ++j)
                {
                y = (r[j] + 1) * smoothed(r[j] + 1) / smoothed(r[j]);
                if (row(r[j] + 1) < 0)
                        indiffValsSeen = TRUE;
                if (! indiffValsSeen)
                        {
                        x = (r[j] + 1) * (next_n = n[row(r[j] + 1)]) / 
                                        (double) n[j];
                        if (fabs(x - y) <= CONFID_FACTOR * sqrt(sq(r[j] + 1.0)
                                        * next_n / (sq((double) n[j]))
                                        * (1 + next_n / (double) n[j])))
                                indiffValsSeen = TRUE;
                        else
                                rStar[j] = x;
                        }
                if (indiffValsSeen)
                        rStar[j] = y;
                }
        bigNprime = 0.0;
        for (j = 0; j < rows; ++j)
                bigNprime += n[j] * rStar[j];
        for (j = 0; j < rows; ++j)
                p[j] = (1 - PZero) * rStar[j] / bigNprime;
        showEstimates();
        }
