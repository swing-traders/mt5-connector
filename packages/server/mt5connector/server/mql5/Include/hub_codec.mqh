// hub_codec.mqh: the hub protocol's binary fields and frame header. It reads the protocol's
// constants and frame kinds, which hub_wire.mqh declares before including it.

union DoubleBits
{
    double value;
    ulong bits;
};

struct HubHeader
{
    uchar version;
    uchar kind;
    ushort flags;
    ulong correlation;
    uint count;
    uint length;
};

// The header's count and length, after version, kind, flags and the correlation id.
#define HUB_COUNT_AT 12
#define HUB_LENGTH_AT 16

// Packs a frame's fields in order, little-endian, a string as its u16 byte count and its UTF-8
// bytes. A string too long for its count sets `failed` and packs nothing.
class BinaryWriter
{
private:
    uchar m_data[];

    void Put(const ulong value, const int width)
    {
        const int at = ArraySize(m_data);
        ArrayResize(m_data, at + width, at + 4096);
        for (int i = 0; i < width; i++)
            m_data[at + i] = (uchar)((value >> (8 * i)) & 0xFF);
    }

    void Patch(const int at, const uint value)
    {
        for (int i = 0; i < 4; i++)
            m_data[at + i] = (uchar)((value >> (8 * i)) & 0xFF);
    }

public:
    bool failed;

    BinaryWriter() : failed(false) {}

    void U8(const uchar value) { Put(value, 1); }
    void U16(const ushort value) { Put(value, 2); }
    void U32(const uint value) { Put(value, 4); }
    void I32(const int value) { Put((ulong)value, 4); }
    void I64(const long value) { Put((ulong)value, 8); }

    // A bool as an integer property reads it, travelling as 0 or 1 in an int64.
    void I64Bool(const long value)
    {
        if (value != 0)
            Put(1, 8);
        else
            Put(0, 8);
    }

    void F64(const double value)
    {
        DoubleBits bits;
        bits.value = value;
        Put(bits.bits, 8);
    }

    void String(const string value)
    {
        uchar bytes[];
        // The count StringToCharArray answers includes the terminating zero it copies.
        const int length = StringToCharArray(value, bytes, 0, WHOLE_ARRAY, CP_UTF8) - 1;
        if (length < 0 || length > 0xFFFF)
        {
            failed = true;
        }
        else
        {
            U16((ushort)length);
            Bytes(bytes, 0, length);
        }
    }

    void Bytes(const uchar &bytes[], const int start, const int count)
    {
        if (count > 0)
        {
            const int at = ArraySize(m_data);
            ArrayResize(m_data, at + count, at + 4096);
            ArrayCopy(m_data, bytes, at, start, count);
        }
    }

    void Tick(const MqlTick &tick)
    {
        uchar bytes[];
        StructToCharArray(tick, bytes);
        Bytes(bytes, 0, ArraySize(bytes));
    }

    void Rates(const MqlRates &rates)
    {
        uchar bytes[];
        StructToCharArray(rates, bytes);
        Bytes(bytes, 0, ArraySize(bytes));
    }

    // A frame's header, its count and length zero until Seal fills them in.
    void Begin(const ENUM_HUB_FRAME kind, const ushort flags, const ulong correlation)
    {
        U8(HUB_VERSION);
        U8((uchar)kind);
        U16(flags);
        Put(correlation, 8);
        U32(0);
        U32(0);
    }

    void Seal(const uint count)
    {
        Patch(HUB_COUNT_AT, count);
        Patch(HUB_LENGTH_AT, (uint)(ArraySize(m_data) - HUB_HEADER_SIZE));
    }

    int Size() const { return ArraySize(m_data); }

    void Slice(const int start, const int count, uchar &bytes[]) const
    {
        ArrayResize(bytes, count);
        if (count > 0)
            ArrayCopy(bytes, m_data, 0, start, count);
    }

    void Take(uchar &bytes[])
    {
        ArrayFree(bytes);
        ArraySwap(bytes, m_data);
        failed = false;
    }
};

// Reads a frame's fields in order. A read past the end sets `failed`, and every read after it
// answers zero or an empty string.
class BinaryReader
{
private:
    uchar m_data[];
    int m_offset;

    ulong Take(const int width)
    {
        ulong value = 0;
        if (failed || m_offset + width > ArraySize(m_data))
        {
            failed = true;
        }
        else
        {
            for (int i = 0; i < width; i++)
                value |= ((ulong)m_data[m_offset + i]) << (8 * i);
            m_offset += width;
        }
        return value;
    }

public:
    bool failed;

    BinaryReader() : m_offset(0), failed(false) {}

    void Load(const uchar &data[])
    {
        ArrayFree(m_data);
        ArrayCopy(m_data, data);
        m_offset = 0;
        failed = false;
    }

    uchar U8() { return (uchar)Take(1); }
    ushort U16() { return (ushort)Take(2); }
    uint U32() { return (uint)Take(4); }
    int I32() { return (int)(uint)Take(4); }
    long I64() { return (long)Take(8); }
    ulong U64() { return Take(8); }

    double F64()
    {
        DoubleBits bits;
        bits.bits = Take(8);
        return bits.value;
    }

    string String()
    {
        const int length = U16();
        string value = "";
        if (failed || m_offset + length > ArraySize(m_data))
        {
            failed = true;
        }
        else
        {
            if (length > 0)
                value = CharArrayToString(m_data, m_offset, length, CP_UTF8);
            m_offset += length;
        }
        return value;
    }

    // False for a frame of another version, or whose length is not the rest of the frame.
    bool Header(HubHeader &header)
    {
        header.version = U8();
        header.kind = U8();
        header.flags = U16();
        header.correlation = U64();
        header.count = U32();
        header.length = U32();
        return !failed && header.version == HUB_VERSION && header.length == (uint)Remaining();
    }

    int Remaining() const { return ArraySize(m_data) - m_offset; }
    bool Done() const { return !failed && m_offset == ArraySize(m_data); }
};
