package com.kni.etl.ketl.smp;

import com.kni.etl.dbutils.ResourcePool;

final class ETLBatchOptimizer extends BatchOptimizer {

    private int batchHistoryPos = 0, bestTimeEverBatchSize = -1, startSize = 25, inc = 100;

    private int[] batchSizeHist = new int[5];

    private long[] batchSizeHistory = new long[5];

    private long bestTimeEver = -1, startTime = -1;

    @Override
    void optimize(ETLWorker worker) {

        worker.tuneInterval = worker.tuneInterval + worker.tuneIntervalIncrement;

        if (worker instanceof ETLReader) {
            if (startTime == -1) {
                for (int i = 0; i < batchSizeHist.length; i++) {
                    batchSizeHist[i] = startSize;
                    startSize += inc;
                }

                worker.batchSize = this.batchSizeHist[batchHistoryPos++];
                startTime = System.nanoTime();
            }
            else {
                long cTime = System.nanoTime();

                if (batchHistoryPos == batchSizeHistory.length) {
                    batchSizeHistory[batchHistoryPos - 1] = cTime - startTime;
                    batchHistoryPos = 0;
                    int bestTime = 0;
                    for (int i = 0; i < this.batchSizeHist.length; i++) {
                        if (this.batchSizeHistory[i] < this.batchSizeHistory[bestTime])
                            bestTime = i;
                    }

                    if (this.bestTimeEver == -1 || this.bestTimeEver > this.batchSizeHistory[bestTime]) {
                        this.bestTimeEverBatchSize = this.batchSizeHist[bestTime];
                        this.bestTimeEver = this.batchSizeHistory[bestTime];
                    }

                    int bestSize = this.batchSizeHist[bestTime];

                    
                    StringBuilder sb = new StringBuilder("Auto-Tune Statistics\n\tBest batchsize:");
                    sb.append(bestSize);
                    sb.append(", time: ");
                    sb.append(this.batchSizeHistory[bestTime]);
                    sb.append("\n\tBest ever batchsize:");
                    sb.append(bestTimeEverBatchSize);
                    sb.append(", time: ");
                    sb.append(bestTimeEver);
                    sb.append("\n\tBatchsize range:" + this.batchSizeHist[1] + " to " + this.batchSizeHist[4]);

                    ResourcePool.LogMessage(Thread.currentThread(),ResourcePool.INFO_MESSAGE,sb.toString());
                    
                    inc = bestSize / 10;
                    startSize = bestSize - (inc * 4);
                    this.batchSizeHist[0] = this.bestTimeEverBatchSize;
                    for (int i = 1; i < this.batchSizeHist.length; i++) {
                        this.batchSizeHist[i] = startSize;
                        startSize += inc;
                    }
                    worker.batchSize = this.batchSizeHist[batchHistoryPos++];
                    startTime = System.nanoTime();
                }
                else {
                    batchSizeHistory[batchHistoryPos - 1] = cTime - startTime;
                    worker.batchSize = this.batchSizeHist[batchHistoryPos++];
                }
                startTime = cTime;

            }
        }
    }

}
