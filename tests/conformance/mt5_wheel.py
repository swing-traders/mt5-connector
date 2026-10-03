"""The pinned MetaTrader5 wheel and the call surface it declares: the constants and helpers its
__init__.py assigns, and the functions and structs its compiled _core defines."""

import ast
import hashlib
import os
import re
import struct
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path

WHEEL_URL = (
    "https://files.pythonhosted.org/packages/b2/f2/"
    "c310381487da4ea67ee6e71bd8244857fe9c276ea37db0c70b5fb280fe4a/"
    "metatrader5-5.0.6231-cp313-cp313-win_amd64.whl"
)
WHEEL_SHA256 = "3a8c8349ce691a4ef05e314afc034ad82f4dbb19415efc7442af5ea075d5e797"
WHEEL_PATH_ENV = "MT5_WHEEL_PATH"

METH_VARARGS = 0x1
METH_KEYWORDS = 0x2
METH_NOARGS = 0x4

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_MAX_ENTRIES = 1000


class WheelDecodeError(Exception):
    """Raised when the wheel does not have the layout this reader decodes."""


@dataclass(frozen=True)
class CoreFunction:
    name: str
    flags: int
    doc: str | None


@dataclass(frozen=True)
class PackageSurface:
    version: str
    constants: dict[str, int]
    helpers: dict[str, ast.FunctionDef]
    star_imports_core: bool
    functions: dict[str, CoreFunction]
    structs: dict[str, tuple[str, ...]]


def fetch_wheel(cache_dir: Path) -> Path:
    """The pinned wheel: MT5_WHEEL_PATH when set, otherwise the cached download."""
    if os.environ.get(WHEEL_PATH_ENV):
        path = Path(os.environ[WHEEL_PATH_ENV])
        _verify(path.read_bytes(), str(path))
        return path
    path = cache_dir / f"{WHEEL_SHA256}.whl"
    if not path.exists():
        with urllib.request.urlopen(WHEEL_URL, timeout=120) as response:
            content = response.read()
        _verify(content, WHEEL_URL)
        partial = path.with_suffix(".partial")
        partial.write_bytes(content)
        partial.replace(path)
    _verify(path.read_bytes(), str(path))
    return path


def _verify(content: bytes, source: str) -> None:
    digest = hashlib.sha256(content).hexdigest()
    if digest != WHEEL_SHA256:
        raise WheelDecodeError(f"{source}: sha256 {digest} is not the pinned {WHEEL_SHA256}")


def read_surface(wheel: Path) -> PackageSurface:
    with zipfile.ZipFile(wheel) as archive:
        init = archive.read("MetaTrader5/__init__.py").decode("utf-8")
        cores = [
            name
            for name in archive.namelist()
            if name.startswith("MetaTrader5/_core.") and name.endswith(".pyd")
        ]
        if len(cores) != 1:
            raise WheelDecodeError(f"expected one compiled _core, found {cores}")
        core = _PeImage(archive.read(cores[0]))
    version, constants, helpers, star_imports_core = _read_init(init)
    return PackageSurface(
        version=version,
        constants=constants,
        helpers=helpers,
        star_imports_core=star_imports_core,
        functions=core.module_functions("_core"),
        structs=core.struct_sequences(),
    )


def _read_init(source: str) -> tuple[str, dict[str, int], dict[str, ast.FunctionDef], bool]:
    version = None
    constants = {}
    helpers = {}
    star_imports_core = False
    for node in ast.parse(source).body:
        if isinstance(node, ast.Assign):
            if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
                raise WheelDecodeError(f"__init__.py: unexpected assignment {ast.unparse(node)}")
            name = node.targets[0].id
            value = _constant_value(name, node.value)
            if name == "__version__":
                version = value
            elif not name.startswith("__"):
                if not isinstance(value, int):
                    raise WheelDecodeError(f"__init__.py: {name} is not an integer")
                constants[name] = value
        elif isinstance(node, ast.ImportFrom):
            if node.level == 1 and node.module == "_core" and node.names[0].name == "*":
                star_imports_core = True
        elif isinstance(node, ast.FunctionDef) and not node.name.startswith("_"):
            helpers[node.name] = node
    if version is None:
        raise WheelDecodeError("__init__.py: no __version__")
    return version, constants, helpers, star_imports_core


def _constant_value(name: str, node: ast.expr) -> object:
    if isinstance(node, ast.Constant):
        return node.value
    elif isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        return -_constant_value(name, node.operand)
    elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        return _constant_value(name, node.left) | _constant_value(name, node.right)
    else:
        raise WheelDecodeError(f"__init__.py: {name} is not a literal: {ast.unparse(node)}")


class _PeImage:
    """A PE32+ image, addressed by the virtual addresses its pointers hold."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        if data[:2] != b"MZ":
            raise WheelDecodeError("_core: not a PE image")
        header = struct.unpack_from("<I", data, 0x3C)[0]
        if data[header : header + 4] != b"PE\0\0":
            raise WheelDecodeError("_core: no PE signature")
        sections = struct.unpack_from("<H", data, header + 6)[0]
        optional_size = struct.unpack_from("<H", data, header + 20)[0]
        optional = header + 24
        if struct.unpack_from("<H", data, optional)[0] != 0x20B:
            raise WheelDecodeError("_core: not a PE32+ image")
        self._base = struct.unpack_from("<Q", data, optional + 24)[0]
        self._sections = []
        for index in range(sections):
            entry = optional + optional_size + 40 * index
            name = data[entry : entry + 8].rstrip(b"\0").decode("ascii")
            _, address, raw_size, raw_offset = struct.unpack_from("<IIII", data, entry + 8)
            self._sections.append((name, address, raw_size, raw_offset))

    def module_functions(self, module: str) -> dict[str, CoreFunction]:
        """The function table of the PyModuleDef whose m_name is the module's name."""
        definitions = self._pointers_to(self._address_of_string(module))
        if len(definitions) != 1:
            raise WheelDecodeError(f"_core: {len(definitions)} references to the name {module!r}")
        # PyModuleDef: m_name, m_doc, m_size, m_methods follow its 40-byte PyModuleDef_Base.
        _, _, _, methods = struct.unpack_from("<QQqQ", self._data, definitions[0])
        offset = self._offset(methods)
        functions = {}
        while offset is not None and len(functions) < _MAX_ENTRIES:
            name, _, flags, doc = struct.unpack_from("<QQI4xQ", self._data, offset)
            if name == 0:
                return functions
            elif doc == 0:
                identifier = self._identifier(name, "function name")
                functions[identifier] = CoreFunction(identifier, flags, None)
            else:
                identifier = self._identifier(name, "function name")
                functions[identifier] = CoreFunction(identifier, flags, self._string(doc))
            offset += 32
        raise WheelDecodeError(f"_core: the {module!r} function table is unreadable")

    def struct_sequences(self) -> dict[str, tuple[str, ...]]:
        """Every PyStructSequence_Desc in the data sections: name, doc, fields, n_in_sequence."""
        structs = {}
        for name, _, raw_size, raw_offset in self._sections:
            if name in (".rdata", ".data"):
                for offset in range(raw_offset, raw_offset + raw_size - 32, 8):
                    found = self._struct_sequence_at(offset)
                    if found is not None:
                        structs[found[0]] = found[1]
        if not structs:
            raise WheelDecodeError("_core: no struct-sequence descriptors")
        return structs

    def _struct_sequence_at(self, offset: int) -> tuple[str, tuple[str, ...]] | None:
        """The descriptor at offset, or None when the bytes there are not one."""
        name, doc, fields, in_sequence = struct.unpack_from("<QQQi", self._data, offset)
        type_name = self._maybe_string(name)
        field_names = self._field_names(fields)
        if (
            type_name is not None
            and _IDENTIFIER.match(type_name)
            and self._maybe_string(doc) is not None
            and field_names is not None
            and len(field_names) == in_sequence
        ):
            return type_name, field_names
        else:
            return None

    def _field_names(self, address: int) -> tuple[str, ...] | None:
        """The names in the PyStructSequence_Field array at address, or None when it is not one."""
        offset = self._offset(address)
        names = []
        while offset is not None and len(names) < _MAX_ENTRIES:
            field_name, _ = struct.unpack_from("<QQ", self._data, offset)
            text = self._maybe_string(field_name)
            if field_name == 0:
                return tuple(names)
            elif text is not None and _IDENTIFIER.match(text):
                names.append(text)
                offset += 16
            else:
                return None
        return None

    def _address_of_string(self, text: str) -> int:
        offset = self._data.find(b"\0" + text.encode("ascii") + b"\0")
        if offset < 0:
            raise WheelDecodeError(f"_core: no string {text!r}")
        return self._address(offset + 1)

    def _pointers_to(self, address: int) -> list[int]:
        packed = struct.pack("<Q", address)
        return [
            offset
            for offset in range(0, len(self._data) - 8, 8)
            if self._data[offset : offset + 8] == packed
        ]

    def _address(self, offset: int) -> int:
        for _, address, raw_size, raw_offset in self._sections:
            if raw_offset <= offset < raw_offset + raw_size:
                return self._base + address + offset - raw_offset
        raise WheelDecodeError(f"_core: file offset {offset:#x} is in no section")

    def _offset(self, address: int) -> int | None:
        relative = address - self._base
        for _, section_address, raw_size, raw_offset in self._sections:
            if section_address <= relative < section_address + raw_size:
                return raw_offset + relative - section_address
        return None

    def _maybe_string(self, address: int) -> str | None:
        offset = self._offset(address)
        if offset is None:
            return None
        end = self._data.find(b"\0", offset)
        try:
            return self._data[offset:end].decode("ascii")
        except UnicodeDecodeError:
            return None

    def _string(self, address: int) -> str:
        text = self._maybe_string(address)
        if text is None:
            raise WheelDecodeError(f"_core: no string at {address:#x}")
        return text

    def _identifier(self, address: int, what: str) -> str:
        text = self._string(address)
        if not _IDENTIFIER.match(text):
            raise WheelDecodeError(f"_core: {what} {text!r} is not an identifier")
        return text
