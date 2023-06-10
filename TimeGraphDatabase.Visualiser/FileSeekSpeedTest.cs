using System.Diagnostics;

namespace TimeGraphDatabase.Visualiser;

public static class FileSeekSpeedTest
{

    public static void Test()
    {
        var sw = new Stopwatch();
        
        foreach (var size in new int[]{1,100,1000,10000,10000,100000,1000000})
        {
            File.Delete("test.bin");
            using var fileStream = new FileStream("test.bin", FileMode.Create);
            var buffer = new byte[size]; // Buffer to hold the zero bytes
            fileStream.Write(buffer, 0, buffer.Length); // Write the zero bytes to the file

            sw.Start();
            for (int i = 0; i < 10000; i++)
            {
                fileStream.Seek(0, SeekOrigin.Begin);
                fileStream.Seek(0, SeekOrigin.End);
            }
            sw.Stop();
            Console.WriteLine($"Start to End:: {sw.Elapsed.TotalSeconds}s to seek over {size} bytes.");
        }
        
        
        foreach (var size in new int[]{1,100,1000,10000,10000,100000,1000000})
        {
            File.Delete("test.bin");
            using var fileStream = new FileStream("test.bin", FileMode.Create);
            var buffer = new byte[size]; // Buffer to hold the zero bytes
            fileStream.Write(buffer, 0, buffer.Length); // Write the zero bytes to the file

            sw.Start();
            for (int i = 0; i < 10000; i++)
            {
                fileStream.Seek(0, SeekOrigin.End);
                fileStream.Seek(0, SeekOrigin.End);
            }
            sw.Stop();
            Console.WriteLine($"End to End:: {sw.Elapsed.TotalSeconds}s to seek over {size} bytes.");
        }
        
        foreach (var size in new int[]{1,100,1000,10000,10000,100000,1000000})
        {
            File.Delete("test.bin");
            using var fileStream = new FileStream("test.bin", FileMode.Create);
            var buffer = new byte[size]; // Buffer to hold the zero bytes
            fileStream.Write(buffer, 0, buffer.Length); // Write the zero bytes to the file

            sw.Start();
            for (int i = 0; i < 10000; i++)
            {
                fileStream.Seek(0, SeekOrigin.End);
                fileStream.Seek(0, SeekOrigin.Begin);
            }
            sw.Stop();
            Console.WriteLine($"End to Start:: {sw.Elapsed.TotalSeconds}s to seek over {size} bytes.");
        }
        
        foreach (var size in new int[]{1,100,1000,10000,10000,100000,1000000})
        {
            File.Delete("test.bin");
            using var fileStream = new FileStream("test.bin", FileMode.Create);
            var buffer = new byte[size]; // Buffer to hold the zero bytes
            fileStream.Write(buffer, 0, buffer.Length); // Write the zero bytes to the file

            sw.Start();
            for (int i = 0; i < 10000; i++)
            {
                fileStream.Seek(0, SeekOrigin.Begin);
                fileStream.Seek(0, SeekOrigin.Begin);
            }
            sw.Stop();
            Console.WriteLine($"Start to Start:: {sw.Elapsed.TotalSeconds}s to seek over {size} bytes.");
        }
        
        foreach (var size in new int[]{1,100,1000,10000,10000,100000,1000000})
        {
            File.Delete("test.bin");
            using var fileStream = new FileStream("test.bin", FileMode.Create);
            var buffer = new byte[size]; // Buffer to hold the zero bytes
            fileStream.Write(buffer, 0, buffer.Length); // Write the zero bytes to the file

            sw.Start();
            for (int i = 0; i < size; i++)
            {
                fileStream.Seek(1, SeekOrigin.Current);
            }
            sw.Stop();
            Console.WriteLine($"Forward 1:: {sw.Elapsed.TotalSeconds/size:0.000000}s to seek over {size} bytes.");
        }
        
        
        foreach (var size in new int[]{1,100,1000,10000,10000,100000,1000000})
        {
            File.Delete("test.bin");
            using var fileStream = new FileStream("test.bin", FileMode.Create);
            var buffer = new byte[size]; // Buffer to hold the zero bytes
            fileStream.Write(buffer, 0, buffer.Length); // Write the zero bytes to the file

            fileStream.Seek(0, SeekOrigin.End);
            sw.Start();
            for (int i = 0; i < size; i++)
            {
                fileStream.Seek(-1, SeekOrigin.Current);
            }
            sw.Stop();
            Console.WriteLine($"Backwards 1:: {sw.Elapsed.TotalSeconds/(double)size:0.00000000}s to seek over {size} bytes.");
        }
    }
}