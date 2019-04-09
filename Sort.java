import java.io.File;
import java.io.RandomAccessFile;

public class Sort
{
    private AsciiData[] read(File file, int numberOfElements, long offset)
    {
        AsciiData[] chunks = new AsciiData[numberOfElements];
        for(int i = 0; i < numberOfElements; i++)
        {
            byte[] data = new byte[100];
            try (RandomAccessFile raf = new RandomAccessFile(file, "r"))
            {
                raf.seek(offset);
                raf.readFully(data);
                offset += 100;
                chunks[i] = new AsciiData(data);
            }
            catch (Exception e)
            {
                System.out.println("read current offset = " + offset);
                e.printStackTrace();
            }
        }
        return chunks;
    }

    private void write(File file, AsciiData[] chunks, long offset)
    {
        byte[] dataToWrite = new byte[chunks.length*100];
        int index = 0;
        for(AsciiData a : chunks)
        {
            for(int i = 0; i < a.array.length(); i++)
            {
                dataToWrite[index++] = (byte)a.array.charAt(i);
            }
        }
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw"))
        {
            raf.seek(offset);
            raf.write(dataToWrite);
        }
        catch (Exception e)
        {
            System.out.println("write current offset = " + offset);
            e.printStackTrace(); }
    }

    public void sortChunks(File input, File output, int numberOfElements, int rounds, long offset)
    {
        QuickSort quickSort = new QuickSort();
        for(int i = 0; i < rounds; i++)
        {
            AsciiData[] chunks = read(input, numberOfElements, offset);
            chunks = quickSort.sort(chunks);
            write(output, chunks, offset);
            offset += numberOfElements * 100;
        }
    }

    public void KWayMerge(File input, File output, int numberOfElements, int numberOfChunks)
    {
        int index = 0;
        long offset = 0;
        long offsetForWrite = 0;
        long numberOfElementLong = numberOfElements;
        AsciiData[] outputBuffer = new AsciiData[numberOfElements];

        IndexMinPQ indexMinPQ = new IndexMinPQ(numberOfChunks);
        long[] currentOffset = new long[numberOfChunks];
        long[] endOffset = new long[numberOfChunks];
        for(int i = 0; i < numberOfChunks; i++)
        {
            long j = i;
            currentOffset[i] = j*(numberOfElements*100);
            endOffset[i] = currentOffset[i] + numberOfElementLong*100;
            AsciiData asciiData = read(input, 1, currentOffset[i])[0];
            indexMinPQ.add(asciiData, i);
            currentOffset[i] += 100;
        }
        while(!indexMinPQ.isEmpty())
        {
            int where = indexMinPQ.getTopIndex();
            AsciiData asciiData = indexMinPQ.popTop();

            if(currentOffset[where] < endOffset[where])
            {
                AsciiData temp = read(input, 1, currentOffset[where])[0];
                indexMinPQ.add(temp, where);

                currentOffset[where] += 100;
            }

            if(index < outputBuffer.length)
            {
                outputBuffer[index++] = asciiData;
            }
            else
            {
                index = 0;
                write(output, outputBuffer, offsetForWrite);
                outputBuffer[index++] = asciiData;
                offsetForWrite += 100 * outputBuffer.length;
            }
        }
        write(output, outputBuffer, offsetForWrite);
    }

    public void sortByThread(File input, File output, int numberOfElement, int numberOfChunks, int threadNumber)
    {
        Thread[] threads = new Thread[threadNumber];
        long offset = 0;
        long workload = (numberOfElement * numberOfChunks) / threadNumber;
        int round = (int)(workload / numberOfElement);
        for(int i = 0; i < threadNumber; i++)
        {
            threads[i] = new Thread(new ThreadSort(input, output, numberOfElement, round, offset, i+1));
            threads[i].start();
            offset += workload * 100;
        }

        for(Thread t : threads)
        {
            try { t.join(); }
            catch (Exception e) { e.printStackTrace(); }
        }
    }

    public static void main(String[] args)
    {
        int numberOfElements = Integer.parseInt(args[0]);
        int numberOfChunks = Integer.parseInt(args[1]);
        int numberOfThreads = Integer.parseInt(args[2]);

        System.out.println("Soring, please wait...");
        String path = System.getProperty("user.dir");
        long startTime = System.currentTimeMillis();

        Sort sort = new Sort();
        File file = new File(path + "/output");
        File sortedFile = new File(path + "/MySortedFile.log");
        sort.sortByThread(file, file, numberOfElements, numberOfChunks, numberOfThreads);
        System.out.println(numberOfChunks + " chunks are sorted");
        sort.KWayMerge(file, sortedFile, numberOfElements, numberOfChunks);

        long endTime = System.currentTimeMillis();
        long totalTime = (endTime - startTime)/1000;
        System.out.println(totalTime + " s");
    }

    class ThreadSort implements Runnable
    {
        private long offset;
        private File input, output;
        private int numberOfElement, rounds;

        public ThreadSort(File input, File output, int numberOfElement, int rounds, long offset, int threadID)
        {
            this.input = input;
            this.output = output;
            this.offset = offset;
            this.numberOfElement = numberOfElement;
            this.rounds = rounds;
            System.out.println("thread = " + threadID + "; offset = " + offset + "; number of element = " +
                    numberOfElement + "; rounds = " + rounds);
        }

        @Override
        public void run()
        {
            sortChunks(input, output, numberOfElement, rounds, offset);
        }
    }

    class AsciiData
    {
        public String array;

        public AsciiData(byte[] input)
        {
            StringBuilder sb = new StringBuilder();
            for(byte b : input) sb.append((char)b);
            array = sb.toString();
        }

        @Override
        public String toString()
        {
            return array;
        }
    }

    public class QuickSort
    {
        private AsciiData[] array;

        public AsciiData[] sort(AsciiData[] input)
        {
            array = input;
            quickSort(0, array.length-1);
            return array;
        }

        private void quickSort(int first, int last)
        {
            if(first < last)
            {
                int q = partition(first, last);
                quickSort(first, q-1);
                quickSort(q+1, last);
            }
        }

        private int partition(int first, int last)
        {
            int i = first - 1;
            String pivot = array[last].array;
            for(int j = first; j < last; j++)
            {
                if(array[j].array.compareTo(pivot) <= 0)
                {
                    i++;
                    exchange(i, j);
                }
            }
            exchange(i+1, last);
            return i+1;
        }

        private void exchange(int i, int j)
        {
            AsciiData temp = array[i];
            array[i] = array[j];
            array[j] = temp;
        }
    }

    public class IndexMinPQ
    {
        private int[] valueArray;
        private AsciiData[] keyArray;
        private int index = 1;
        private int size = 0;

        public IndexMinPQ(int length)
        {
            valueArray = new int[length];
            keyArray = new AsciiData[length];
        }

        public void add(AsciiData key, int value)
        {
            if(index >= keyArray.length) enlargeArray();
            keyArray[index] = key;
            valueArray[index] = value;
            swim(index);
            index++;
            size++;
        }

        public int getTopIndex()
        {
            return valueArray[1];
        }

        public AsciiData popTop()
        {
            AsciiData top = keyArray[1];
            exchange(1, index-1);
            index--;
            sink(1, index);
            size--;
            return top;
        }

        public boolean isEmpty() { return size == 0; }

        private void swim(int index)
        {
            while (index/2 > 0)
            {
                if (keyArray[index].array.compareTo(keyArray[index / 2].array) < 0)
                {
                    exchange(index, index/2);
                    index = index / 2;
                }
                else break;
            }
        }

        private void sink(int index, int length)
        {
            if (index > length - 1) return;
            while (index * 2 < length - 1)
            {
                int smallest = index;
                int left_child = index * 2;
                int right_child = index * 2 + 1;
                if (left_child < length &&
                        keyArray[left_child].array.compareTo( keyArray[smallest].array) < 0)
                    smallest = left_child;
                if (right_child < length &&
                        keyArray[right_child].array.compareTo(keyArray[smallest].array) < 0)
                    smallest = right_child;

                if (smallest != index)
                {
                    exchange(smallest, index);
                    index = smallest;
                }
                else break;
            }
        }

        private void exchange(int i, int j)
        {
            int temp1 = valueArray[i];
            valueArray[i] = valueArray[j];
            valueArray[j] = temp1;
            AsciiData temp2 = keyArray[i];
            keyArray[i] = keyArray[j];
            keyArray[j] = temp2;
        }

        //Flexible array.
        private void enlargeArray()
        {
            int length = keyArray.length * 2;
            AsciiData[] newKeyArray = new AsciiData[length];
            int[] newValueArray = new int[length];
            for(int i = 0; i < keyArray.length; i++)
            {
                newKeyArray[i] = keyArray[i];
                newValueArray[i] = valueArray[i];
            }
            keyArray = newKeyArray;
            valueArray = newValueArray;
        }
    }
}
