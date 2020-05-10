using Newtonsoft.Json;
using StackExchange.Redis;
using StackExchange.Redis.KeyspaceIsolation;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace ConsoleNetCore
{
    public static class TestRedis
    {
        private const int _dataCount = 10000;

        private const int _testCount = 100;

        private const string _namespace = "Student";

        private const string _keyAll = "all";

        private const string _connection = "localhost";

        private static readonly IConnectionMultiplexer _redis = ConnectionMultiplexer.Connect(_connection);

        private static readonly IDatabase _database = _redis.GetDatabase();

        private static readonly IDatabase _namespaceDatabase = _database.WithKeyPrefix(_namespace);

        private static readonly IServer _server = _redis.GetServer(_redis.GetEndPoints().FirstOrDefault());

        public static void TestGetSetDataUsingAll()
        {
            SetDataAsString(true);

            Console.WriteLine("Get Data ...");

            var getDataResult = new List<ExecutingTime>();

            for(int i=0; i<_testCount; i++)
            {
                var iResult = GetDataUsingAll();
                getDataResult.Add(iResult);
            }

            var getKeyTime = getDataResult.Sum(x => x.GetKeyTime) / _testCount;
            var getDataTime = getDataResult.Sum(x => x.GetDataTime) / _testCount;
            var deserializeDataTime = getDataResult.Sum(x => x.DeserializeDataTime) / _testCount;

            Console.WriteLine($"Get Key take: {getKeyTime} ms");
            Console.WriteLine($"Get Data take: {getDataTime} ms");
            Console.WriteLine($"Deserialize Data take: {deserializeDataTime} ms");
        }

        public static void TestGetSetDataUsingKeys()
        {
            SetDataAsString(false);

            Console.WriteLine("Get Data ...");

            var getDataResult = new List<ExecutingTime>();

            for (int i = 0; i < _testCount; i++)
            {
                var iResult = GetDataUsingKeys();
                getDataResult.Add(iResult);
            }

            var getKeyTime = getDataResult.Sum(x => x.GetKeyTime) / _testCount;
            var getDataTime = getDataResult.Sum(x => x.GetDataTime) / _testCount;
            var deserializeDataTime = getDataResult.Sum(x => x.DeserializeDataTime) / _testCount;

            Console.WriteLine($"Get Key take: {getKeyTime} ms");
            Console.WriteLine($"Get Data take: {getDataTime} ms");
            Console.WriteLine($"Deserialize Data take: {deserializeDataTime} ms");
        }

        public static void TestGetSetDataUsingAllWithNamespace()
        {
            SetDataAsStringWithNamespace(true);

            Console.WriteLine("Get Data ...");

            var getDataResult = new List<ExecutingTime>();

            for (int i = 0; i < _testCount; i++)
            {
                var iResult = GetDataUsingAllWithNamespace();
                getDataResult.Add(iResult);
            }

            var getKeyTime = getDataResult.Sum(x => x.GetKeyTime) / _testCount;
            var getDataTime = getDataResult.Sum(x => x.GetDataTime) / _testCount;
            var deserializeDataTime = getDataResult.Sum(x => x.DeserializeDataTime) / _testCount;

            Console.WriteLine($"Get Key take: {getKeyTime} ms");
            Console.WriteLine($"Get Data take: {getDataTime} ms");
            Console.WriteLine($"Deserialize Data take: {deserializeDataTime} ms");
        }

        public static void TestGetSetDataUsingKeysWithNamespace()
        {
            SetDataAsStringWithNamespace(false);

            Console.WriteLine("Get Data ...");

            var getDataResult = new List<ExecutingTime>();

            for (int i = 0; i < _testCount; i++)
            {
                var iResult = GetDataUsingKeysWithNamespace();
                getDataResult.Add(iResult);
            }

            var getKeyTime = getDataResult.Sum(x => x.GetKeyTime) / _testCount;
            var getDataTime = getDataResult.Sum(x => x.GetDataTime) / _testCount;
            var deserializeDataTime = getDataResult.Sum(x => x.DeserializeDataTime) / _testCount;

            Console.WriteLine($"Get Key take: {getKeyTime} ms");
            Console.WriteLine($"Get Data take: {getDataTime} ms");
            Console.WriteLine($"Deserialize Data take: {deserializeDataTime} ms");
        }

        public static void TestGetSetDataAsHash()
        {
            SetDataAsHash();

            Console.WriteLine("Get Data ...");

            var getDataResult = new List<ExecutingTime>();

            for (int i = 0; i < _testCount; i++)
            {
                var iResult = GetDataAsHash();
                getDataResult.Add(iResult);
            }

            var getKeyTime = getDataResult.Sum(x => x.GetKeyTime) / _testCount;
            var getDataTime = getDataResult.Sum(x => x.GetDataTime) / _testCount;
            var deserializeDataTime = getDataResult.Sum(x => x.DeserializeDataTime) / _testCount;

            Console.WriteLine($"Get Key take: {getKeyTime} ms");
            Console.WriteLine($"Get Data take: {getDataTime} ms");
            Console.WriteLine($"Deserialize Data take: {deserializeDataTime} ms");
        }

        public static void TestGetSetDataAsList()
        {
            SetDataAsList();

            Console.WriteLine("Get Data ...");

            var getDataResult = new List<ExecutingTime>();

            for (int i = 0; i < _testCount; i++)
            {
                var iResult = GetDataAsList();
                getDataResult.Add(iResult);
            }

            var getKeyTime = getDataResult.Sum(x => x.GetKeyTime) / _testCount;
            var getDataTime = getDataResult.Sum(x => x.GetDataTime) / _testCount;
            var deserializeDataTime = getDataResult.Sum(x => x.DeserializeDataTime) / _testCount;

            Console.WriteLine($"Get Key take: {getKeyTime} ms");
            Console.WriteLine($"Get Data take: {getDataTime} ms");
            Console.WriteLine($"Deserialize Data take: {deserializeDataTime} ms");
        }

        private static void SetDataAsString(bool isSetAll = false)
        {
            Console.WriteLine("Set Data ...");
            Console.WriteLine("Flush All DB ...");
            _server.Execute("FLUSHALL");

            var data = GenerateDataAsList();

            var stopWatch = Stopwatch.StartNew();

            var dataRedis = data.ToDictionary(x => MakeKey(x.Id), x => (RedisValue)JsonConvert.SerializeObject(x)).ToArray();

            _database.StringSet(dataRedis);

            if (isSetAll)
            {
                _database.StringSet(MakeKey(_keyAll), JsonConvert.SerializeObject(data));
            }

            stopWatch.Stop();
            Console.WriteLine($"Set data take: {stopWatch.ElapsedMilliseconds} ms");
        }

        private static void SetDataAsStringWithNamespace(bool isSetAll = false)
        {
            Console.WriteLine("Set Data ...");
            Console.WriteLine("Flush All DB ...");
            _server.Execute("FLUSHALL");

            var data = GenerateDataAsList();

            var stopWatch = Stopwatch.StartNew();

            var dataRedis = data.ToDictionary(x => (RedisKey) x.Id.ToString(), x => (RedisValue)JsonConvert.SerializeObject(x)).ToArray();

            _namespaceDatabase.StringSet(dataRedis);

            if (isSetAll)
            {
                _namespaceDatabase.StringSet(_keyAll, JsonConvert.SerializeObject(data));
            }

            stopWatch.Stop();
            Console.WriteLine($"Set data take: {stopWatch.ElapsedMilliseconds} ms");
        }

        private static void SetDataAsHash()
        {
            Console.WriteLine("Set Data ...");
            Console.WriteLine("Flush All DB ...");
            _server.Execute("FLUSHALL");

            var data = GenerateDataAsList();

            var stopWatch = Stopwatch.StartNew();

            var dataRedis = data.Select(x => new HashEntry(x.Id.ToString(), JsonConvert.SerializeObject(x))).ToArray();

            _database.HashSet(_namespace, dataRedis);

            stopWatch.Stop();
            Console.WriteLine($"Set data take: {stopWatch.ElapsedMilliseconds} ms");
        }

        private static void SetDataAsList()
        {
            Console.WriteLine("Set Data ...");
            Console.WriteLine("Flush All DB ...");
            _server.Execute("FLUSHALL");

            var data = GenerateDataAsList();

            var stopWatch = Stopwatch.StartNew();

            var dataRedis = data.Select(x => (RedisValue) JsonConvert.SerializeObject(x)).ToArray();

            _database.ListRightPush(_namespace, dataRedis);

            stopWatch.Stop();
            Console.WriteLine($"Set data take: {stopWatch.ElapsedMilliseconds} ms");
        }

        private static ExecutingTime GetDataUsingAll()
        {
            var result = new ExecutingTime();

            var stopWatch = Stopwatch.StartNew();

            var keyAll = MakeKey(_keyAll);

            stopWatch.Stop();
            result.GetKeyTime = stopWatch.ElapsedMilliseconds;
            stopWatch.Restart();

            var data = _database.StringGet(keyAll);

            stopWatch.Stop();
            result.GetDataTime = stopWatch.ElapsedMilliseconds;
            stopWatch.Restart();

            var dataResult = JsonConvert.DeserializeObject<List<Student>>(data);

            if (dataResult.Count != _dataCount)
                throw new Exception("Missing data");

            stopWatch.Stop();
            result.DeserializeDataTime = stopWatch.ElapsedMilliseconds;

            return result;
        }

        private static ExecutingTime GetDataUsingKeys()
        {
            var result = new ExecutingTime();

            var stopWatch = Stopwatch.StartNew();

            var allKeys = _server.Keys(_database.Database, $"{_namespace}:*").ToArray();

            stopWatch.Stop();
            result.GetKeyTime = stopWatch.ElapsedMilliseconds;
            stopWatch.Restart();

            var data = _database.StringGet(allKeys);

            stopWatch.Stop();
            result.GetDataTime = stopWatch.ElapsedMilliseconds;
            stopWatch.Restart();

            foreach(var item in data)
            {
                JsonConvert.DeserializeObject<Student>(item);
            }

            if (data.Length != _dataCount)
                throw new Exception("Missing data");

            stopWatch.Stop();
            result.DeserializeDataTime = stopWatch.ElapsedMilliseconds;

            return result;
        }

        private static ExecutingTime GetDataUsingAllWithNamespace()
        {
            var result = new ExecutingTime();

            var stopWatch = Stopwatch.StartNew();

            var keyAll = _keyAll;

            stopWatch.Stop();
            result.GetKeyTime = stopWatch.ElapsedMilliseconds;
            stopWatch.Restart();

            var data = _namespaceDatabase.StringGet(keyAll);

            stopWatch.Stop();
            result.GetDataTime = stopWatch.ElapsedMilliseconds;
            stopWatch.Restart();

            var dataResult = JsonConvert.DeserializeObject<List<Student>>(data);

            if (dataResult.Count != _dataCount)
                throw new Exception("Missing data");

            stopWatch.Stop();
            result.DeserializeDataTime = stopWatch.ElapsedMilliseconds;

            return result;
        }

        private static ExecutingTime GetDataUsingKeysWithNamespace()
        {
            var result = new ExecutingTime();

            var stopWatch = Stopwatch.StartNew();

            var allKeys = _server.Keys(_namespaceDatabase.Database, "*").Select(x => (RedisKey) x.ToString().Replace(_namespace, string.Empty)).ToArray();

            stopWatch.Stop();
            result.GetKeyTime = stopWatch.ElapsedMilliseconds;
            stopWatch.Restart();

            var data = _namespaceDatabase.StringGet(allKeys);

            stopWatch.Stop();
            result.GetDataTime = stopWatch.ElapsedMilliseconds;
            stopWatch.Restart();

            foreach (var item in data)
            {
                JsonConvert.DeserializeObject<Student>(item);
            }

            if (data.Length != _dataCount)
                throw new Exception("Missing data");

            stopWatch.Stop();
            result.DeserializeDataTime = stopWatch.ElapsedMilliseconds;

            return result;
        }

        private static ExecutingTime GetDataAsHash()
        {
            var result = new ExecutingTime();

            var stopWatch = Stopwatch.StartNew();

            var keyAll = _namespace;

            stopWatch.Stop();
            result.GetKeyTime = stopWatch.ElapsedMilliseconds;
            stopWatch.Restart();

            var data = _database.HashGetAll(keyAll);

            stopWatch.Stop();
            result.GetDataTime = stopWatch.ElapsedMilliseconds;
            stopWatch.Restart();

            foreach (var item in data)
            {
                JsonConvert.DeserializeObject<Student>(item.Value);
            }

            if (data.Length != _dataCount)
                throw new Exception("Missing data");

            stopWatch.Stop();
            result.DeserializeDataTime = stopWatch.ElapsedMilliseconds;

            return result;
        }

        private static ExecutingTime GetDataAsList()
        {
            var result = new ExecutingTime();

            var stopWatch = Stopwatch.StartNew();

            var keyAll = _namespace;

            stopWatch.Stop();
            result.GetKeyTime = stopWatch.ElapsedMilliseconds;
            stopWatch.Restart();

            var data = _database.ListRange(keyAll);

            stopWatch.Stop();
            result.GetDataTime = stopWatch.ElapsedMilliseconds;
            stopWatch.Restart();

            foreach (var item in data)
            {
                JsonConvert.DeserializeObject<Student>(item);
            }

            if (data.Length != _dataCount)
                throw new Exception("Missing data");

            stopWatch.Stop();
            result.DeserializeDataTime = stopWatch.ElapsedMilliseconds;

            return result;
        }

        private static List<Student> GenerateDataAsList()
        {
            var result = new List<Student>();

            for (int i = 0; i < _dataCount; i++)
            {
                result.Add(new Student(Guid.NewGuid(), i % 17));
            }

            return result;
        }

        private static Dictionary<Guid, Student> GenerateDataAsDictionary()
        {
            var result = new Dictionary<Guid, Student>();

            for (int i = 0; i < _dataCount; i++)
            {
                var id = Guid.NewGuid();
                result[id] = new Student(id, i % 17);
            }

            return result;
        }

        private static RedisKey MakeKey(Guid id)
        {
            return $"{_namespace}:{id}";
        }

        private static RedisKey MakeKey(string suffix)
        {
            return $"{_namespace}:{suffix}";
        }
    }

    public class Student
    {
        public Student()
        {

        }

        public Student(Guid id, int classId)
        {
            Id = id;
            Name = $"Student {id}";
            Class = $"Class {classId}";
            DOB = DateTime.Today.AddDays(classId);
        }

        public Guid Id { get; set; }
        public string Name { get; set; }
        public string Class { get; set; }
        public DateTime DOB { get; set; }
    }

    public class ExecutingTime
    {
        public long GetKeyTime { get; set; }
        public long GetDataTime { get; set; }
        public long DeserializeDataTime { get; set; }
    }
}
