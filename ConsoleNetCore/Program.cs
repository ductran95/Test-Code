using System;

namespace ConsoleNetCore
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Test Get Data Using All ...");
            TestRedis.TestGetSetDataUsingAll();
            Console.WriteLine();

            Console.WriteLine("Test Get Data Using Keys ...");
            TestRedis.TestGetSetDataUsingKeys();
            Console.WriteLine();

            Console.WriteLine("Test Get Data Using All With Namespace ...");
            TestRedis.TestGetSetDataUsingAllWithNamespace();
            Console.WriteLine();

            Console.WriteLine("Test Get Data Using Keys With Namespace ...");
            TestRedis.TestGetSetDataUsingKeysWithNamespace();
            Console.WriteLine();

            Console.WriteLine("Test Get Data As Hash ...");
            TestRedis.TestGetSetDataAsHash();
            Console.WriteLine();

            Console.WriteLine("Test Get Data As List ...");
            TestRedis.TestGetSetDataAsList();
            Console.WriteLine();
        }
    }
}
