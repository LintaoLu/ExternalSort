import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class ExternalSort
{
    private File originalFile;
    private File temp;
    //How many element we have.
    private long totalElements;

    public ExternalSort(long totalElements)
    {
        //Set up path.
        originalFile = new File("D:\\output");
        temp = new File("D:\\temp");
        this.totalElements = totalElements;
    }

    //Input: offset (where to start copy), size (how many lines to copy).
    //Output: An array of AsciiData.
    public AsciiData[] readAsciiDataToMemory(long offset, int number)
    {
        ReadFileToMemory readFileToMemory = new ReadFileToMemory(offset, number);
        return readFileToMemory.getChunk();
    }

    //Write sorted data to temp file.
    public void writeAsciiDataToDisk(AsciiData[] asciiData, long offset)
    {
        new WriteFileToDisk(asciiData, offset);
    }

    //Quick sort AsciiData and return back.
    public AsciiData[] sort(AsciiData[] input)
    {
        QuickSort q = new QuickSort();
        return q.sort(input);
    }

    public void createThread(int number)
    {
        long workload = totalElements/number;
        int size = 1000000000;
        int rounds = (int) workload / size;
        int offset = 0;
        if(workload < 1000000000)
        {
            size = (int) workload;
            rounds = 1;
        }
        for(int i = 0; i < number; i++)
        {
            System.out.println(offset);
            Thread thread = new Thread(new ThreadsSort(offset, size, rounds));
            thread.start();
            offset += (workload*100);
        }
    }

    public static void main(String[] args)
    {
        ExternalSort externalSort = new ExternalSort(100);
        externalSort.createThread(4);
    }

    /**
     * Input: int offset of file (form where to load data to memory);
     *        int size, how much data to load each time
     *        int rounds, size of the whole data.
     * Output: our temp file with sorted data.
     * */
    private class ThreadsSort implements Runnable
    {
        private long offset = 0;
        private int size = 0;
        private int rounds = 0;
        private AsciiData[] fileData;

        public ThreadsSort(long offset, int size, int rounds)
        {
            this.offset = offset;
            this.size = size;
            this.rounds = rounds;
        }

        @Override
        public void run()
        {
            for(int i = 0; i < rounds; i++)
            {
                fileData = readAsciiDataToMemory(offset, size);
                fileData = sort(fileData);
                writeAsciiDataToDisk(fileData, offset);
                //Read from next chunk.
                offset += size*100;
            }
        }
    }

    /**
     * This class save a reference to byte[] and an short variable that tell us
     * the value (sum of all Ascii in byte[]) of this byte array.
     * Size of this class can be regarded as 102 bytes, though it actually not.
     */
    class AsciiData
    {
        public byte[] array;
        public short value = 0;
        public AsciiData (byte[] input)
        {
            array = input;
            for(byte b : input) value += b;
        }
    }

    /**
     * Input: a chunk of AsciiData, and offset.
     * Write this chuck of data to file in respect of offset.
     */
    private class WriteFileToDisk
    {
        private AsciiData[] chunk;

        public WriteFileToDisk(AsciiData[] input, long offset)
        {
            chunk = input;
            writeData(offset);
        }

        private void writeData(long offset)
        {
            for(AsciiData asciiData : chunk)
            {
                try { writePortion(temp, offset, asciiData.array); }
                catch (Exception e) { e.printStackTrace(); }
                offset += 100;
            }
        }

        private void writePortion(File file, long offset, byte[] data) throws IOException
        {
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw"))
            {
                raf.seek(offset);
                raf.write(data);
            }
        }
    }

    /**
     * Input: offset (where to start copy) and size (how many lines to copy).
     * Output: an array of AsciiData.
     * Should be private because it will use File path.
     * */
    private class ReadFileToMemory
    {
        private AsciiData[] chunk;

        public ReadFileToMemory(long offset, int number)
        {
            chunk = new AsciiData[number];
            loadData(offset, number);
        }

        //From offset of file,load 'number' lines of 100 bytes data.
        private void loadData(long offset, int number)
        {
            for(int i = 0; i < number; i++)
            {
                try { chunk[i] = new AsciiData(readPortion(originalFile, offset+(i*100), 100)); }
                catch (Exception e) { System.out.println("Size is too big!"); }
            }
        }

        private byte[] readPortion(File file, long offset, int length) throws IOException
        {
            byte[] data = new byte[length];
            try (RandomAccessFile raf = new RandomAccessFile(file, "r"))
            {
                raf.seek(offset);
                raf.readFully(data);
            }
            return data;
        }

        public AsciiData[] getChunk() { return chunk; }
    }

    /**
     * Quick Sort algorithms for AsciiData.
     * */
    class QuickSort
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
            short pivot = array[last].value;
            for(int j = first; j < last; j++)
            {
                if(array[j].value <= pivot)
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
}