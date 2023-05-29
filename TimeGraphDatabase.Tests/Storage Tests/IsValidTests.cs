using System.Xml.Xsl;
using FluentAssertions;
using TimeGraphDatabase.Engine;

namespace TimeGraphDatabase.Tests;

public class IsValidTests : BaseStorageTest
{

    [Fact]
    public async Task ValidFile()
    {
        await GivenAFileContaining(1, 2, 3, 4 ,5 ,6 ,7, 8, 9, 10, 11);
        var storage = new Storage();
        var isValid = storage.IsValid();
        isValid.Should().Be(true);
    }
    
    [Fact]
    public async Task InvalidIfTheFileContainsADuplicate()
    {
        await GivenAFileContaining(1, 2, 3, 4 ,5 ,6 ,7, 8, 9, 10, 10);
        var storage = new Storage();
        var isValid = storage.IsValid();
        isValid.Should().Be(false);
    }
    
    [Fact]
    public async Task InvalidIfTheFileContainsOutOfOrderEntries()
    {
        await GivenAFileContaining(1, 2, 3, 7 ,5 ,6 ,4, 8, 9, 10, 11);
        var storage = new Storage();
        var isValid = storage.IsValid();
        isValid.Should().Be(false);
    }
}