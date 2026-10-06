"""Drives the MT5 terminal's dialogs through Win32 messages by control identity — window class,
control id, caption, and the command ids the executable's own menu resources hold."""

from __future__ import annotations

import argparse
import ctypes
import os
import re
import sys
import time
from collections.abc import Callable, Iterable, Sequence
from ctypes import wintypes
from dataclasses import dataclass
from enum import IntEnum, IntFlag, StrEnum


class Action(StrEnum):
    MAIN_WINDOW = "main-window"
    SEARCH_SERVER = "search-server"
    ALLOW_WEBREQUEST = "allow-webrequest"


class WindowClass(StrEnum):
    # The terminal's main window.
    TERMINAL = "MetaQuotes::MetaTrader::5.00"
    DIALOG = "#32770"
    BUTTON = "Button"
    EDIT = "Edit"
    LIST_VIEW = "SysListView32"
    TAB_CONTROL = "SysTabControl32"


class Message(IntEnum):
    WM_SETTEXT = 0x000C
    WM_GETTEXT = 0x000D
    WM_GETTEXTLENGTH = 0x000E
    WM_KEYDOWN = 0x0100
    WM_KEYUP = 0x0101
    WM_COMMAND = 0x0111
    WM_LBUTTONDOWN = 0x0201
    WM_LBUTTONUP = 0x0202
    WM_LBUTTONDBLCLK = 0x0203
    BM_GETCHECK = 0x00F0
    BM_CLICK = 0x00F5
    PSM_SETCURSEL = 0x0465
    PSM_GETCURRENTPAGEHWND = 0x0476
    TCM_GETITEMCOUNT = 0x1304
    TCM_GETCURSEL = 0x130B
    TCM_GETITEMW = 0x133C
    LVM_GETITEMCOUNT = 0x1004
    LVM_GETITEMRECT = 0x100E


class Command(IntEnum):
    OK = 1
    CANCEL = 2


class CheckState(IntEnum):
    UNCHECKED = 0
    CHECKED = 1


class WindowRelation(IntEnum):
    OWNER = 4


class VirtualKey(IntEnum):
    RETURN = 0x0D


class MouseKey(IntFlag):
    LEFT_BUTTON = 0x0001


class Keystroke(IntFlag):
    REPEATED_ONCE = 0x00000001
    WAS_DOWN = 0x40000000
    RELEASED = 0x80000000


class SendFlag(IntFlag):
    ABORT_IF_HUNG = 0x0002


class MenuFlag(IntFlag):
    BY_POSITION = 0x0400


class ResourceType(IntEnum):
    MENU = 4


class LoadFlag(IntFlag):
    AS_DATAFILE = 0x00000002
    AS_IMAGE_RESOURCE = 0x00000020


class ProcessAccess(IntFlag):
    VM_OPERATION = 0x0008
    VM_READ = 0x0010
    VM_WRITE = 0x0020


class Allocation(IntFlag):
    COMMIT = 0x00001000
    RESERVE = 0x00002000


class FreeType(IntFlag):
    RELEASE = 0x00008000


class PageProtection(IntEnum):
    READWRITE = 0x04


class TabItemField(IntFlag):
    TEXT = 0x0001


class ItemRectPart(IntEnum):
    LABEL = 2


# Tools > Options: a property sheet captioned "Options".
OPTIONS_MENU_PATH = ("Tools", "Options")
OPTIONS_CAPTION = "Options"
OPTIONS_TABS_ID = 12320  # the Options dialog's SysTabControl32
EXPERTS_TAB = "Experts"
# The Options dialog's Experts page: "Allow WebRequest for listed URL", and the list of those URLs,
# whose last row adds one.
WEBREQUEST_CHECKBOX_ID = 10322
WEBREQUEST_LIST_ID = 10191

# File > Open an Account: a wizard whose first page searches the companies' trade servers.
ACCOUNT_MENU_PATH = ("File", "Open an Account")
SERVER_SEARCH_BOX_ID = 10814  # the account dialog's first page: the search Edit
SERVER_SEARCH_BUTTON_ID = 10815  # the account dialog's first page: "Find your company"

MESSAGE_TIMEOUT_MS = 5000
MAIN_WINDOW_SECONDS = 10
DIALOG_SECONDS = 10
EDITOR_SECONDS = 5
SEARCH_SECONDS = 10
POLL_SECONDS = 0.1
TAB_TEXT_CHARS = 128


class DialogError(Exception):
    """A window, control or menu command the terminal should hold is absent or does not respond."""


@dataclass(frozen=True)
class MenuEntry:
    """An item of a menu resource: a command, or a submenu holding `items`."""

    label: str
    command_id: int | None
    items: tuple[MenuEntry, ...]


def menu_label(text: str) -> str:
    """The label a menu item reads as: its accelerator dropped, its `&` mnemonic markers removed."""
    return re.sub(r"&(.)", r"\1", text.split("\t", 1)[0])


def command_id(menus: Iterable[Sequence[MenuEntry]], path: Sequence[str]) -> int:
    """The command id `path`'s labels lead to, which must be the same in every menu holding it."""
    found = set()
    for menu in menus:
        for entry in _entries_at(menu, path):
            if entry.command_id is not None:
                found.add(entry.command_id)
    named = " > ".join(path)
    if not found:
        raise DialogError(f"the terminal's menus hold no {named}")
    if len(found) > 1:
        raise DialogError(f"the terminal's menus give {named} the ids {sorted(found)}")
    return found.pop()


def _entries_at(entries: Sequence[MenuEntry], path: Sequence[str]) -> list[MenuEntry]:
    matching = [entry for entry in entries if menu_label(entry.label) == path[0]]
    if len(path) == 1:
        return matching
    return [found for entry in matching for found in _entries_at(entry.items, path[1:])]


def tab_index(names: Sequence[str], name: str) -> int:
    """The index of the one tab named `name`."""
    indices = [index for index, tab in enumerate(names) if tab == name]
    if len(indices) != 1:
        raise DialogError(f"the Options dialog has {len(indices)} tabs named {name}, not one")
    return indices[0]


class _TabItem(ctypes.Structure):
    _fields_ = [
        ("mask", wintypes.UINT),
        ("state", wintypes.DWORD),
        ("state_mask", wintypes.DWORD),
        ("text", ctypes.c_void_p),
        ("text_capacity", ctypes.c_int),
        ("image", ctypes.c_int),
        ("param", wintypes.LPARAM),
    ]


def _prototype(function, restype, *argtypes) -> None:
    function.restype = restype
    function.argtypes = argtypes


user32 = ctypes.WinDLL("user32", use_last_error=True)
kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
WNDENUMPROC = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
ENUMRESNAMEPROCW = ctypes.WINFUNCTYPE(
    wintypes.BOOL, wintypes.HMODULE, ctypes.c_void_p, ctypes.c_void_p, wintypes.LPARAM
)
_prototype(user32.EnumWindows, wintypes.BOOL, WNDENUMPROC, wintypes.LPARAM)
_prototype(user32.EnumChildWindows, wintypes.BOOL, wintypes.HWND, WNDENUMPROC, wintypes.LPARAM)
_prototype(user32.GetClassNameW, ctypes.c_int, wintypes.HWND, wintypes.LPWSTR, ctypes.c_int)
_prototype(user32.GetWindowTextW, ctypes.c_int, wintypes.HWND, wintypes.LPWSTR, ctypes.c_int)
_prototype(user32.GetWindowTextLengthW, ctypes.c_int, wintypes.HWND)
_prototype(user32.GetWindow, wintypes.HWND, wintypes.HWND, wintypes.UINT)
_prototype(user32.GetParent, wintypes.HWND, wintypes.HWND)
_prototype(user32.GetDlgCtrlID, ctypes.c_int, wintypes.HWND)
_prototype(user32.IsWindow, wintypes.BOOL, wintypes.HWND)
_prototype(user32.IsWindowVisible, wintypes.BOOL, wintypes.HWND)
_prototype(user32.IsWindowEnabled, wintypes.BOOL, wintypes.HWND)
_prototype(user32.GetWindowThreadProcessId, wintypes.DWORD, wintypes.HWND, wintypes.LPDWORD)
_prototype(
    user32.SendMessageTimeoutW,
    wintypes.LPARAM,
    wintypes.HWND,
    wintypes.UINT,
    wintypes.WPARAM,
    wintypes.LPARAM,
    wintypes.UINT,
    wintypes.UINT,
    ctypes.POINTER(ctypes.c_size_t),
)
_prototype(
    user32.PostMessageW,
    wintypes.BOOL,
    wintypes.HWND,
    wintypes.UINT,
    wintypes.WPARAM,
    wintypes.LPARAM,
)
_prototype(user32.LoadMenuW, wintypes.HMENU, wintypes.HMODULE, ctypes.c_void_p)
_prototype(user32.DestroyMenu, wintypes.BOOL, wintypes.HMENU)
_prototype(user32.GetMenuItemCount, ctypes.c_int, wintypes.HMENU)
_prototype(
    user32.GetMenuStringW,
    ctypes.c_int,
    wintypes.HMENU,
    wintypes.UINT,
    wintypes.LPWSTR,
    ctypes.c_int,
    wintypes.UINT,
)
_prototype(user32.GetMenuItemID, wintypes.UINT, wintypes.HMENU, ctypes.c_int)
_prototype(user32.GetSubMenu, wintypes.HMENU, wintypes.HMENU, ctypes.c_int)
_prototype(
    kernel32.LoadLibraryExW, wintypes.HMODULE, wintypes.LPCWSTR, wintypes.HANDLE, wintypes.DWORD
)
_prototype(kernel32.FreeLibrary, wintypes.BOOL, wintypes.HMODULE)
_prototype(
    kernel32.EnumResourceNamesW,
    wintypes.BOOL,
    wintypes.HMODULE,
    ctypes.c_void_p,
    ENUMRESNAMEPROCW,
    wintypes.LPARAM,
)
_prototype(kernel32.OpenProcess, wintypes.HANDLE, wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
_prototype(kernel32.CloseHandle, wintypes.BOOL, wintypes.HANDLE)
_prototype(
    kernel32.VirtualAllocEx,
    ctypes.c_void_p,
    wintypes.HANDLE,
    ctypes.c_void_p,
    ctypes.c_size_t,
    wintypes.DWORD,
    wintypes.DWORD,
)
_prototype(
    kernel32.VirtualFreeEx,
    wintypes.BOOL,
    wintypes.HANDLE,
    ctypes.c_void_p,
    ctypes.c_size_t,
    wintypes.DWORD,
)
_prototype(
    kernel32.ReadProcessMemory,
    wintypes.BOOL,
    wintypes.HANDLE,
    ctypes.c_void_p,
    ctypes.c_void_p,
    ctypes.c_size_t,
    ctypes.POINTER(ctypes.c_size_t),
)
_prototype(
    kernel32.WriteProcessMemory,
    wintypes.BOOL,
    wintypes.HANDLE,
    ctypes.c_void_p,
    ctypes.c_void_p,
    ctypes.c_size_t,
    ctypes.POINTER(ctypes.c_size_t),
)


class _RemoteMemory:
    """A buffer in the process owning a window, for a message whose argument points at memory: the
    terminal reads and writes such an argument in its own address space."""

    def __init__(self, window: int, size: int) -> None:
        process_id = wintypes.DWORD()
        user32.GetWindowThreadProcessId(window, ctypes.byref(process_id))
        access = ProcessAccess.VM_OPERATION | ProcessAccess.VM_READ | ProcessAccess.VM_WRITE
        self._process = kernel32.OpenProcess(access, False, process_id.value)
        if not self._process:
            raise ctypes.WinError(ctypes.get_last_error())
        self.address = kernel32.VirtualAllocEx(
            self._process,
            None,
            size,
            Allocation.COMMIT | Allocation.RESERVE,
            PageProtection.READWRITE,
        )
        if not self.address:
            error = ctypes.get_last_error()
            kernel32.CloseHandle(self._process)
            raise ctypes.WinError(error)

    def __enter__(self) -> _RemoteMemory:
        return self

    def __exit__(self, *exc) -> None:
        kernel32.VirtualFreeEx(self._process, self.address, 0, FreeType.RELEASE)
        kernel32.CloseHandle(self._process)

    def write(self, offset: int, source: ctypes.Structure) -> None:
        size = ctypes.sizeof(source)
        if not kernel32.WriteProcessMemory(
            self._process, self.address + offset, ctypes.byref(source), size, None
        ):
            raise ctypes.WinError(ctypes.get_last_error())

    def read(self, offset: int, target: ctypes.Structure | ctypes.Array) -> None:
        size = ctypes.sizeof(target)
        if not kernel32.ReadProcessMemory(
            self._process, self.address + offset, ctypes.byref(target), size, None
        ):
            raise ctypes.WinError(ctypes.get_last_error())


def _send(window: int, message: Message, wparam: int = 0, lparam: int = 0) -> int:
    result = ctypes.c_size_t()
    if not user32.SendMessageTimeoutW(
        window,
        message,
        wparam,
        lparam,
        SendFlag.ABORT_IF_HUNG,
        MESSAGE_TIMEOUT_MS,
        ctypes.byref(result),
    ):
        raise DialogError(f"the terminal did not answer {message.name}")
    return result.value


def _post(window: int, message: Message, wparam: int = 0, lparam: int = 0) -> None:
    if not user32.PostMessageW(window, message, wparam, lparam):
        raise ctypes.WinError(ctypes.get_last_error())


def _wait_for[T](find: Callable[[], T | None], seconds: float, failure: str) -> T:
    """Poll `find` until it returns a value, raising `failure` once `seconds` have passed."""
    deadline = time.monotonic() + seconds
    while True:
        found = find()
        if found is not None:
            return found
        if time.monotonic() >= deadline:
            raise DialogError(failure)
        time.sleep(POLL_SECONDS)


def _only[T](values: Iterable[T]) -> T | None:
    """The one value of `values`, or None for none or several."""
    found = list(values)
    if len(found) == 1:
        return found[0]
    return None


def _top_windows() -> list[int]:
    windows = []

    def collect(window: int, _param: int) -> bool:
        windows.append(window)
        return True

    user32.EnumWindows(WNDENUMPROC(collect), 0)
    return windows


def _descendants(parent: int) -> list[int]:
    windows = []

    def collect(window: int, _param: int) -> bool:
        windows.append(window)
        return True

    user32.EnumChildWindows(parent, WNDENUMPROC(collect), 0)
    return windows


def _class_name(window: int) -> str:
    buffer = ctypes.create_unicode_buffer(256)
    user32.GetClassNameW(window, buffer, len(buffer))
    return buffer.value


def _caption(window: int) -> str:
    buffer = ctypes.create_unicode_buffer(user32.GetWindowTextLengthW(window) + 1)
    user32.GetWindowTextW(window, buffer, len(buffer))
    return buffer.value


def _control(parent: int, control_id: int, window_class: WindowClass, holder: str) -> int:
    """The one control under `parent` with `control_id` and `window_class`."""
    matches = [
        window
        for window in _descendants(parent)
        if user32.GetDlgCtrlID(window) == control_id and _class_name(window) == window_class
    ]
    if not matches:
        raise DialogError(f"{holder} has no {window_class} {control_id}")
    if len(matches) > 1:
        raise DialogError(f"{holder} has {len(matches)} {window_class} {control_id}")
    return matches[0]


def _main_window() -> int:
    """The terminal's one visible main window."""
    windows = [
        window
        for window in _top_windows()
        if user32.IsWindowVisible(window) and _class_name(window) == WindowClass.TERMINAL
    ]
    if len(windows) != 1:
        raise DialogError(f"{len(windows)} terminal main windows, not one")
    return windows[0]


def _enabled_main_window() -> int:
    """The terminal's main window once no modal dialog disables it."""
    window = _main_window()
    return _wait_for(
        lambda: window if user32.IsWindowEnabled(window) else None,
        MAIN_WINDOW_SECONDS,
        f"the terminal's main window stayed disabled for {MAIN_WINDOW_SECONDS}s",
    )


def _owned_dialogs(owner: int) -> set[int]:
    return {
        window
        for window in _top_windows()
        if user32.IsWindowVisible(window)
        and _class_name(window) == WindowClass.DIALOG
        and user32.GetWindow(window, WindowRelation.OWNER) == owner
    }


def _menus() -> list[tuple[MenuEntry, ...]]:
    """Every menu resource of the terminal's executable, read without running its code."""
    module = kernel32.LoadLibraryExW(
        os.environ["MT5_TERMINAL_PATH"],
        None,
        LoadFlag.AS_DATAFILE | LoadFlag.AS_IMAGE_RESOURCE,
    )
    if not module:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        names = []

        # A name is an integer id, or a string the enumeration owns only for the callback's span.
        def collect(_module: int, _kind: int, name: int, _param: int) -> bool:
            if name >> 16 == 0:
                names.append(name)
            else:
                names.append(ctypes.wstring_at(name))
            return True

        kernel32.EnumResourceNamesW(module, ResourceType.MENU, ENUMRESNAMEPROCW(collect), 0)
        menus = []
        for name in names:
            menu = user32.LoadMenuW(module, name)
            if not menu:
                raise ctypes.WinError(ctypes.get_last_error())
            try:
                menus.append(_menu_entries(menu))
            finally:
                user32.DestroyMenu(menu)
        return menus
    finally:
        kernel32.FreeLibrary(module)


def _menu_entries(menu: int) -> tuple[MenuEntry, ...]:
    entries = []
    for position in range(user32.GetMenuItemCount(menu)):
        buffer = ctypes.create_unicode_buffer(256)
        user32.GetMenuStringW(menu, position, buffer, len(buffer), MenuFlag.BY_POSITION)
        submenu = user32.GetSubMenu(menu, position)
        if submenu:
            entries.append(MenuEntry(buffer.value, None, _menu_entries(submenu)))
        else:
            entries.append(MenuEntry(buffer.value, user32.GetMenuItemID(menu, position), ()))
    return tuple(entries)


def _open_dialog(owner: int, path: Sequence[str], name: str) -> int:
    """Run the menu command at `path` and return the dialog it opens."""
    command = command_id(_menus(), path)
    before = _owned_dialogs(owner)
    _post(owner, Message.WM_COMMAND, command)
    return _wait_for(
        lambda: _only(_owned_dialogs(owner) - before),
        DIALOG_SECONDS,
        f"{name} did not open within {DIALOG_SECONDS}s",
    )


def _close_dialog(dialog: int, command: Command, name: str) -> None:
    _post(dialog, Message.WM_COMMAND, command)
    _wait_for(
        lambda: None if user32.IsWindow(dialog) else True,
        DIALOG_SECONDS,
        f"{name} did not close within {DIALOG_SECONDS}s",
    )


def _current_page(dialog: int, name: str) -> int:
    page = _send(dialog, Message.PSM_GETCURRENTPAGEHWND)
    if not page:
        raise DialogError(f"{name} shows no page")
    return page


def _set_text(control: int, text: str, name: str) -> None:
    """Put `text` in an edit control, confirmed by reading it back."""
    buffer = ctypes.create_unicode_buffer(text)
    _send(control, Message.WM_SETTEXT, 0, ctypes.addressof(buffer))
    held = ctypes.create_unicode_buffer(_send(control, Message.WM_GETTEXTLENGTH) + 1)
    _send(control, Message.WM_GETTEXT, len(held), ctypes.addressof(held))
    if held.value != text:
        raise DialogError(f"{name} did not take its text")


def _tab_names(tabs: int) -> list[str]:
    names = []
    with _RemoteMemory(tabs, 4096) as memory:
        text_offset = ctypes.sizeof(_TabItem)
        for index in range(_send(tabs, Message.TCM_GETITEMCOUNT)):
            memory.write(
                0,
                _TabItem(
                    mask=TabItemField.TEXT,
                    text=memory.address + text_offset,
                    text_capacity=TAB_TEXT_CHARS,
                ),
            )
            if not _send(tabs, Message.TCM_GETITEMW, index, memory.address):
                raise DialogError(f"the Options dialog's tab {index} has no name")
            text = ctypes.create_unicode_buffer(TAB_TEXT_CHARS)
            memory.read(text_offset, text)
            names.append(text.value)
    return names


def _select_tab(dialog: int, tabs: int, name: str) -> int:
    """Turn the property sheet to the page of the tab named `name` and return that page."""
    index = tab_index(_tab_names(tabs), name)
    _send(dialog, Message.PSM_SETCURSEL, index)
    page = _current_page(dialog, "the Options dialog")
    if _send(tabs, Message.TCM_GETCURSEL) != index:
        raise DialogError(f"the Options dialog did not turn to its {name} page")
    return page


def _open_row_editor(url_list: int, row: int) -> int:
    """Double-click inside `row`'s label, as the list reports its bounds, and return the in-place
    editor the click opens. A click on the boundary between the row's two columns opens nothing."""
    bounds = wintypes.RECT(left=ItemRectPart.LABEL)
    with _RemoteMemory(url_list, ctypes.sizeof(bounds)) as memory:
        memory.write(0, bounds)
        if not _send(url_list, Message.LVM_GETITEMRECT, row, memory.address):
            raise DialogError(f"the WebRequest list has no row {row}")
        memory.read(0, bounds)
    x = (bounds.left + bounds.right) // 2
    y = (bounds.top + bounds.bottom) // 2
    point = (y << 16) | (x & 0xFFFF)
    _post(url_list, Message.WM_LBUTTONDOWN, MouseKey.LEFT_BUTTON, point)
    _post(url_list, Message.WM_LBUTTONUP, 0, point)
    _post(url_list, Message.WM_LBUTTONDBLCLK, MouseKey.LEFT_BUTTON, point)
    _post(url_list, Message.WM_LBUTTONUP, 0, point)
    return _wait_for(
        lambda: _only(
            window
            for window in _descendants(url_list)
            if user32.GetParent(window) == url_list and _class_name(window) == WindowClass.EDIT
        ),
        EDITOR_SECONDS,
        f"the WebRequest list's add row opened no editor within {EDITOR_SECONDS}s",
    )


def search_server(server: str) -> None:
    """Search `server` on the account dialog's first page, which adds its company's trade servers to
    the terminal's server list, then cancel the dialog."""
    dialog = _open_dialog(_enabled_main_window(), ACCOUNT_MENU_PATH, "the account dialog")
    page = _current_page(dialog, "the account dialog")
    search_box = _control(page, SERVER_SEARCH_BOX_ID, WindowClass.EDIT, "the account dialog")
    find = _control(page, SERVER_SEARCH_BUTTON_ID, WindowClass.BUTTON, "the account dialog")
    _set_text(search_box, server, "the account dialog's search box")
    # The page disables its button while a search runs, whatever the search finds.
    _send(find, Message.BM_CLICK)
    if user32.IsWindowEnabled(find):
        raise DialogError("the account dialog's search did not start")
    _wait_for(
        lambda: True if user32.IsWindowEnabled(find) else None,
        SEARCH_SECONDS,
        f"the account dialog's search did not finish within {SEARCH_SECONDS}s",
    )
    _close_dialog(dialog, Command.CANCEL, "the account dialog")
    print("mt5: the account dialog searched the trade server", flush=True)


def allow_webrequest(url: str) -> None:
    """Tick "Allow WebRequest for listed URL" on the Options dialog's Experts page, add `url` to its
    list, which must then hold that URL alone, and apply the dialog."""
    dialog = _open_dialog(_enabled_main_window(), OPTIONS_MENU_PATH, "the Options dialog")
    if _caption(dialog) != OPTIONS_CAPTION:
        raise DialogError("the Options command opened a dialog not captioned Options")
    tabs = _control(dialog, OPTIONS_TABS_ID, WindowClass.TAB_CONTROL, "the Options dialog")
    page = _select_tab(dialog, tabs, EXPERTS_TAB)
    holder = f"the Options dialog's {EXPERTS_TAB} page"
    checkbox = _control(page, WEBREQUEST_CHECKBOX_ID, WindowClass.BUTTON, holder)
    url_list = _control(page, WEBREQUEST_LIST_ID, WindowClass.LIST_VIEW, holder)
    if _send(checkbox, Message.BM_GETCHECK) == CheckState.UNCHECKED:
        _send(checkbox, Message.BM_CLICK)
    if _send(checkbox, Message.BM_GETCHECK) != CheckState.CHECKED:
        raise DialogError("the Options dialog's WebRequest checkbox did not tick")
    rows = _send(url_list, Message.LVM_GETITEMCOUNT)
    editor = _open_row_editor(url_list, rows - 1)
    _set_text(editor, url, "the WebRequest list's editor")
    _post(editor, Message.WM_KEYDOWN, VirtualKey.RETURN, Keystroke.REPEATED_ONCE)
    _post(
        editor,
        Message.WM_KEYUP,
        VirtualKey.RETURN,
        Keystroke.REPEATED_ONCE | Keystroke.WAS_DOWN | Keystroke.RELEASED,
    )
    _wait_for(
        lambda: None if user32.IsWindow(editor) else True,
        EDITOR_SECONDS,
        f"the WebRequest list's editor did not close within {EDITOR_SECONDS}s",
    )

    # The list's text cannot be read: its row count, the add row last, is the only confirmation.
    def changed_row_count() -> int | None:
        count = _send(url_list, Message.LVM_GETITEMCOUNT)
        if count != rows:
            return count
        return None

    committed = _wait_for(
        changed_row_count,
        EDITOR_SECONDS,
        f"the WebRequest list took no URL within {EDITOR_SECONDS}s",
    )
    if committed != rows + 1:
        raise DialogError(f"the WebRequest list went from {rows} rows to {committed} on one URL")
    if committed - 1 != 1:
        raise DialogError(f"the WebRequest list holds {committed - 1} URLs, not one")
    _close_dialog(dialog, Command.OK, "the Options dialog")
    print(f"mt5: the WebRequest allowlist holds {url} alone", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Drive the MT5 terminal's dialogs.")
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser(Action.MAIN_WINDOW, help="Exit 0 once the terminal's main window is up.")
    actions.add_parser(
        Action.SEARCH_SERVER,
        help="Search MT5_SERVER in the account dialog, adding its company's servers.",
    )
    webrequest = actions.add_parser(
        Action.ALLOW_WEBREQUEST, help="Allow WebRequest for one URL, on a list holding none."
    )
    webrequest.add_argument("url")
    args = parser.parse_args()
    action = Action(args.action)
    try:
        if action == Action.MAIN_WINDOW:
            _main_window()
            print("mt5: the terminal's main window is up", flush=True)
        elif action == Action.SEARCH_SERVER:
            search_server(os.environ["MT5_SERVER"])
        else:
            allow_webrequest(args.url)
    except DialogError as error:
        print(f"mt5: {error}", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
