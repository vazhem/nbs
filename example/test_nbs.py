#!/usr/bin/env python3

import os
import subprocess
import time
import signal
import sys
import logging
import argparse
from typing import List, Optional, Tuple
import tempfile
import random

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/test_nbs.log')
    ]
)

logger = logging.getLogger(__name__)

class ProcessManager:
    """Класс для управления фоновыми процессами"""

    def __init__(self):
        self.processes = {}  # Словарь для хранения процессов {name: (process, pid)}

    def add_process(self, name: str, process: subprocess.Popen) -> None:
        """Добавить процесс в менеджер"""
        self.processes[name] = (process, process.pid)
        logger.info(f"Добавлен процесс: {name} с PID: {process.pid}")

    def kill_process_with_sudo(self, pid: int, signal: int = signal.SIGTERM) -> bool:
        """
        Завершить процесс с использованием sudo, если обычное завершение не работает

        Args:
            pid: PID процесса для завершения
            signal: Сигнал для отправки

        Returns:
            bool: True если успешно, False если ошибка
        """
        try:
            # Сначала пробуем обычное завершение
            try:
                os.kill(pid, signal)
                return True
            except PermissionError:
                logger.debug(f"Нет прав для завершения процесса {pid}, пробуем через sudo")

            # Если нет прав, используем sudo
            result = subprocess.run(
                ["sudo", "kill", f"-{signal}", str(pid)],
                capture_output=True,
                text=True,
                timeout=5
            )

            if result.returncode == 0:
                logger.info(f"Процесс {pid} успешно завершен через sudo")
                return True
            else:
                logger.warning(f"Не удалось завершить процесс {pid} через sudo: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            logger.warning(f"Таймаут при завершении процесса {pid} через sudo")
            return False
        except Exception as e:
            logger.debug(f"Ошибка при завершении процесса {pid}: {e}")
            return False

    def find_all_related_processes(self, root_pid: int) -> List[int]:
        """
        Найти все связанные процессы, включая все дочерние и процессы sudo

        Args:
            root_pid: Корневой PID для поиска

        Returns:
            List[int]: Список всех связанных PID
        """
        all_pids = []
        pids_to_check = [root_pid]
        checked_pids = set()

        while pids_to_check:
            current_pid = pids_to_check.pop(0)
            if current_pid in checked_pids:
                continue

            checked_pids.add(current_pid)

            # Находим прямых потомков
            children = get_process_tree(current_pid)
            for child_pid in children:
                if child_pid not in checked_pids:
                    pids_to_check.append(child_pid)
                    all_pids.append(child_pid)

        return all_pids

    def terminate_process_group(self, name: str, process: subprocess.Popen, pid: int) -> None:
        """Завершить процесс и всю его группу процессов"""
        try:
            if process.poll() is None:  # Если процесс еще работает
                logger.info(f"Завершение процесса {name} и его дочерних процессов (PID: {pid})")

                # Находим все связанные процессы
                related_pids = self.find_all_related_processes(pid)
                logger.info(f"Найдено {len(related_pids)} связанных процессов: {related_pids}")

                # Отправляем SIGTERM всей группе процессов (отрицательный PID)
                try:
                    os.killpg(os.getpgid(pid), signal.SIGTERM)
                    logger.info(f"Отправлен SIGTERM группе процессов {name}")
                except ProcessLookupError:
                    # Если процесс уже завершился, пробуем завершить через объект процесса
                    process.terminate()

                # Завершаем все связанные процессы (используя sudo если нужно)
                for related_pid in related_pids:
                    self.kill_process_with_sudo(related_pid, signal.SIGTERM)

                # Даем время на корректное завершение
                try:
                    process.wait(timeout=15)
                    logger.info(f"Процесс {name} и его дочерние процессы успешно завершены")
                except subprocess.TimeoutExpired:
                    logger.warning(f"Принудительное завершение процесса {name} и его дочерних процессов")
                    try:
                        # Отправляем SIGKILL всей группе процессов
                        os.killpg(os.getpgid(pid), signal.SIGKILL)
                    except ProcessLookupError:
                        process.kill()

                    # Принудительно завершаем связанные процессы
                    for related_pid in related_pids:
                        self.kill_process_with_sudo(related_pid, signal.SIGKILL)

                    process.wait()
                    logger.info(f"Процесс {name} и его дочерние процессы принудительно завершены")
        except Exception as e:
            logger.error(f"Ошибка при завершении процесса {name}: {e}")

    def terminate_all(self) -> None:
        """Завершить все управляемые процессы в обратном порядке запуска"""
        logger.info("Завершение всех фоновых процессов...")

        # Завершаем процессы в обратном порядке (LIFO - Last In, First Out)
        process_names = list(self.processes.keys())
        for name in reversed(process_names):
            if name in self.processes:  # Проверяем, что процесс еще в словаре
                process, pid = self.processes[name]
                self.terminate_process_group(name, process, pid)

        self.processes.clear()

def run_command(command: List[str], cwd: str = None, check: bool = True) -> Tuple[int, str, str]:
    """
    Выполнить команду и вернуть код возврата, stdout и stderr

    Args:
        command: Список аргументов команды
        cwd: Рабочая директория
        check: Если True, выбросить исключение при ненулевом коде возврата

    Returns:
        Tuple[int, str, str]: (код возврата, stdout, stderr)
    """
    logger.info(f"Выполнение команды: {' '.join(command)}")
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            capture_output=True,
            text=True,
            check=check
        )

        if result.stdout:
            logger.info(f"STDOUT:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"STDERR:\n{result.stderr}")

        return result.returncode, result.stdout, result.stderr
    except subprocess.CalledProcessError as e:
        logger.error(f"Команда завершилась с ошибкой (код: {e.returncode})")
        if e.stdout:
            logger.error(f"STDOUT:\n{e.stdout}")
        if e.stderr:
            logger.error(f"STDERR:\n{e.stderr}")
        if check:
            raise
        return e.returncode, e.stdout, e.stderr

def run_background_command(
    command: List[str],
    output_file: str,
    cwd: str = None
) -> subprocess.Popen:
    """
    Запустить команду в фоновом режиме с перенаправлением вывода в файл

    Args:
        command: Список аргументов команды
        output_file: Файл для перенаправления вывода
        cwd: Рабочая директория

    Returns:
        subprocess.Popen: Объект процесса
    """
    logger.info(f"Запуск фонового процесса: {' '.join(command)}")
    logger.info(f"Вывод будет перенаправлен в: {output_file}")

    # Убедимся, что директория для логов существует
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, 'w') as f:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            stdout=f,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid  # Создаем новую сессию для группы процессов
        )

    logger.info(f"Процесс запущен с PID: {process.pid}")
    return process

def check_process_running(pid: int, process_name: str = None) -> bool:
    """
    Проверить, работает ли процесс с указанным PID

    Args:
        pid: PID процесса
        process_name: Имя процесса для логирования

    Returns:
        bool: True если процесс работает
    """
    try:
        # Отправляем сигнал 0, чтобы проверить существование процесса
        os.kill(pid, 0)
        logger.info(f"Процесс {process_name or pid} (PID: {pid}) работает")
        return True
    except ProcessLookupError:
        logger.warning(f"Процесс {process_name or pid} (PID: {pid}) не найден")
        return False
    except PermissionError:
        logger.warning(f"Нет доступа к процессу {process_name or pid} (PID: {pid})")
        return False

def get_process_tree(pid: int) -> List[int]:
    """
    Получить список дочерних процессов для указанного PID (упрощенная версия)

    Args:
        pid: PID родительского процесса

    Returns:
        List[int]: Список PID дочерних процессов
    """
    try:
        # Используем /proc для получения информации о дочерних процессах
        if os.path.exists(f"/proc/{pid}"):
            try:
                # Читаем содержимое директории /proc/[pid]/task/[tid]/children
                # Это более надежный способ, чем вызов внешних команд
                children_content = ""
                task_dir = f"/proc/{pid}/task"
                if os.path.exists(task_dir):
                    for task_id in os.listdir(task_dir):
                        children_file = f"{task_dir}/{task_id}/children"
                        if os.path.exists(children_file):
                            with open(children_file, 'r') as f:
                                children_content += f.read().strip()

                if children_content:
                    return [int(pid_str) for pid_str in children_content.split() if pid_str.strip()]
            except Exception as e:
                logger.debug(f"Не удалось прочитать /proc для PID {pid}: {e}")
    except Exception as e:
        logger.debug(f"Не удалось получить дерево процессов для PID {pid}: {e}")

    return []

def check_process_group_running(pid: int, process_name: str = None) -> Tuple[bool, List[int]]:
    """
    Проверить, работает ли процесс и его дочерние процессы

    Args:
        pid: PID процесса
        process_name: Имя процесса для логирования

    Returns:
        Tuple[bool, List[int]]: (родительский процесс работает, список дочерних PID)
    """
    parent_running = check_process_running(pid, process_name)
    child_pids = []

    if parent_running:
        try:
            child_pids = get_process_tree(pid)
            if child_pids:
                logger.info(f"Найдены дочерние процессы для {process_name or pid}: {child_pids}")
        except Exception as e:
            logger.debug(f"Не удалось проверить дочерние процессы для {process_name or pid}: {e}")

    return parent_running, child_pids

def wait_for_log_entry(log_file: str, target_message: str, timeout: int = 30, check_interval: float = 0.5) -> bool:
    """
    Ожидать появления указанного сообщения в лог файле

    Args:
        log_file: Путь к лог файлу
        target_message: Целевое сообщение для поиска
        timeout: Таймаут ожидания в секундах
        check_interval: Интервал проверки в секундах

    Returns:
        bool: True если сообщение найдено, False если таймаут
    """
    logger.info(f"Ожидание сообщения '{target_message}' в файле {log_file}")

    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            if os.path.exists(log_file):
                with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    if target_message in content:
                        logger.info(f"Найдено сообщение: '{target_message}'")
                        return True
        except Exception as e:
            logger.debug(f"Ошибка при чтении лог файла {log_file}: {e}")

        time.sleep(check_interval)

    logger.error(f"Сообщение '{target_message}' не найдено в течение {timeout} секунд")
    return False

def generate_random_file(file_path: str, size_kb: int) -> None:
    """
    Сгенерировать файл с случайными данными указанного размера с использованием dd

    Args:
        file_path: Путь к файлу для создания
        size_kb: Размер в килобайтах
    """
    size_bytes = size_kb * 1024
    block_size = 4 * 1024  # 4KB
    count_blocks = size_bytes // block_size

    logger.info(f"Генерация файла с случайными данными: {file_path} (размер: {size_kb}KB = {size_bytes} bytes)")
    logger.info(f"Используемые параметры dd: bs=4k, count={count_blocks}")

    # Используем dd с /dev/urandom для генерации случайных данных
    dd_command = [
        "sudo",
        "dd",
        "if=/dev/urandom",
        f"of={file_path}",
        f"bs=4k",
        f"count={count_blocks}",
        "status=progress"
    ]

    returncode, stdout, stderr = run_command(dd_command, check=False)

    if returncode == 0:
        logger.info(f"Файл {file_path} успешно сгенерирован")
    else:
        raise RuntimeError(f"Не удалось сгенерировать файл {file_path}: {stderr}")

def write_to_device(file_path: str, device_path: str, offset_mb: int) -> None:
    """
    Записать данные из файла в устройство с указанным смещением с использованием dd

    Args:
        file_path: Путь к файлу с данными
        device_path: Путь к устройству
        offset_mb: Смещение в мегабайтах
    """
    offset_bytes = offset_mb * 1024 * 1024
    block_size = 4 * 1024  # 4KB
    seek_blocks = offset_bytes // block_size

    logger.info(f"Запись данных из {file_path} в {device_path} со смещением {offset_mb}MB ({offset_bytes} bytes)")
    logger.info(f"Используемые параметры dd: bs=4k, seek={seek_blocks}")

    # Используем dd для записи с смещением в блоках по 4KB
    dd_command = [
        "sudo",
        "dd",
        f"if={file_path}",
        f"of={device_path}",
        f"bs=4k",
        f"seek={seek_blocks}",
        "conv=notrunc",
        "status=progress"
    ]

    returncode, stdout, stderr = run_command(dd_command, check=False)

    if returncode == 0:
        logger.info(f"Данные успешно записаны в {device_path}")
    else:
        raise RuntimeError(f"Не удалось записать данные в {device_path}: {stderr}")

def read_from_device(device_path: str, file_path: str, offset_mb: int, size_kb: int) -> None:
    """
    Прочитать данные из устройства в файл с указанным смещением с использованием dd

    Args:
        device_path: Путь к устройству
        file_path: Путь к файлу для сохранения данных
        offset_mb: Смещение в мегабайтах
        size_kb: Размер для чтения в килобайтах
    """
    offset_bytes = offset_mb * 1024 * 1024
    size_bytes = size_kb * 1024
    block_size = 4 * 1024  # 4KB
    skip_blocks = offset_bytes // block_size
    count_blocks = size_bytes // block_size

    logger.info(f"Чтение данных из {device_path} в {file_path} со смещением {offset_mb}MB ({offset_bytes} bytes), размер: {size_kb}KB ({size_bytes} bytes)")
    logger.info(f"Используемые параметры dd: bs=4k, skip={skip_blocks}, count={count_blocks}")

    # Используем dd для чтения с смещением в блоках по 4KB
    dd_command = [
        "sudo",
        "dd",
        f"if={device_path}",
        f"of={file_path}",
        f"bs=4k",
        f"skip={skip_blocks}",
        f"count={count_blocks}",
        "status=progress"
    ]

    returncode, stdout, stderr = run_command(dd_command, check=False)

    if returncode == 0:
        logger.info(f"Данные успешно прочитаны из {device_path} и сохранены в {file_path}")
    else:
        raise RuntimeError(f"Не удалось прочитать данные из {device_path}: {stderr}")

def compare_files(file1_path: str, file2_path: str) -> bool:
    """
    Сравнить два файла с использованием cmp

    Args:
        file1_path: Путь к первому файлу
        file2_path: Путь ко второму файлу

    Returns:
        bool: True если файлы идентичны
    """
    logger.info(f"Сравнение файлов: {file1_path} и {file2_path}")

    # Используем cmp для сравнения файлов
    cmp_command = ["cmp", file1_path, file2_path]

    returncode, stdout, stderr = run_command(cmp_command, check=False)

    if returncode == 0:
        logger.info("Файлы идентичны")
        return True
    else:
        logger.error(f"Файлы различаются: {stderr}")
        return False

def cleanup_temp_files(*file_paths: str) -> None:
    """
    Удалить временные файлы

    Args:
        *file_paths: Пути к файлам для удаления
    """
    for file_path in file_paths:
        try:
            if os.path.exists(file_path):
                # Сначала пробуем удалить без sudo
                try:
                    os.remove(file_path)
                    logger.info(f"Временный файл удален: {file_path}")
                except PermissionError:
                    # Если нет прав, используем sudo
                    logger.debug(f"Нет прав для удаления файла {file_path}, пробуем через sudo")
                    rm_command = ["sudo", "rm", "-f", file_path]
                    returncode, stdout, stderr = run_command(rm_command, check=False)
                    if returncode == 0:
                        logger.info(f"Временный файл удален через sudo: {file_path}")
                    else:
                        logger.warning(f"Не удалось удалить временный файл {file_path} через sudo: {stderr}")
        except Exception as e:
            logger.warning(f"Не удалось удалить временный файл {file_path}: {e}")

def parse_arguments():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description='Тестовый скрипт для NBS')
    parser.add_argument('--wait', action='store_true',
                       help='Не завершать процессы после выполнения тестов, ожидать сигнала Ctrl-C')
    return parser.parse_args()

def main():
    """Основная функция скрипта"""
    args = parse_arguments()

    logger.info("Начало выполнения тестового скрипта NBS")

    # Убедимся, что мы в правильной директории
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    logger.info(f"Рабочая директория: {os.getcwd()}")

    # Создаем менеджер процессов
    process_manager = ProcessManager()

    try:
        # Шаг 1: Запуск 0-setup.sh
        logger.info("=== Шаг 1: Запуск 0-setup.sh ===")
        returncode, stdout, stderr = run_command(["./0-setup.sh"], check=True)
        logger.info(f"0-setup.sh успешно выполнен")

        # Шаг 2: Запуск 1-start_storage.sh в фоне
        logger.info("=== Шаг 2: Запуск 1-start_storage.sh в фоне ===")
        ydbd_process = run_background_command(
            ["./1-start_storage.sh"],
            "logs/ydbd.log"
        )
        process_manager.add_process("ydbd", ydbd_process)

        # Шаг 3: Пауза и проверка что ydbd процесс работает
        logger.info("=== Шаг 3: Пауза и проверка ydbd процесса ===")
        time.sleep(1)
        if not check_process_running(ydbd_process.pid, "ydbd"):
            raise RuntimeError("ydbd процесс не запущен или завершился с ошибкой")
        logger.info("ydbd процесс работает корректно")

        # Шаг 4: Запуск 2-init_storage.sh
        logger.info("=== Шаг 4: Запуск 2-init_storage.sh ===")
        returncode, stdout, stderr = run_command(["./2-init_storage.sh"], check=True)
        logger.info(f"2-init_storage.sh успешно выполнен")

        # Шаг 5: Запуск 3-start_nbs.sh в фоне
        logger.info("=== Шаг 5: Запуск 3-start_nbs.sh в фоне ===")
        nbsd_process = run_background_command(
            ["./3-start_nbs.sh"],
            "logs/nbsd.log"
        )
        process_manager.add_process("nbsd", nbsd_process)

        time.sleep(10)
        if not check_process_running(nbsd_process.pid, "nbsd"):
            raise RuntimeError("nbsd процесс не запущен или завершился с ошибкой")
        logger.info("nbsd процесс работает корректно")

        # Шаг 6: Создание диска
        logger.info("=== Шаг 6: Создание диска ===")
        returncode, stdout, stderr = run_command(
            ["./5-create_disk.sh", "--kind=ssd_direct", "--disk-id=vdd10"],
            check=True
        )
        logger.info(f"Диск vdd10 успешно создан")

        # Шаг 6.1: Ожидание инициализации раздела
        logger.info("=== Шаг 6.1: Ожидание инициализации раздела ===")
        if not wait_for_log_entry("logs/nbsd.log", "Partition fully initialized and ready for IO", timeout=10):
            raise RuntimeError("Раздел не был инициализирован в течение 5 секунд")
        logger.info("Раздел успешно инициализирован и готов к операциям ввода-вывода")

        # Шаг 7: Подключение диска в фоне
        logger.info("=== Шаг 7: Подключение диска в фоне ===")
        attach_process = run_background_command(
            ["./6-attach_disk.sh", "--disk-id", "vdd10", "-d", "/dev/nbd0"],
            "logs/attach.log"
        )
        process_manager.add_process("attach", attach_process)

        time.sleep(5)
        if not check_process_running(attach_process.pid, "attach_disk"):
            raise RuntimeError("attach_disk процесс не запущен или завершился с ошибкой")
        logger.info("attach_disk процесс работает корректно")

        # Шаг 8: Генерация файла со случайными данными (200MB)
        logger.info("=== Шаг 8: Генерация файла со случайными данными (200MB) ===")
        original_file = "/tmp/random_data_200mb.bin"
        file_size_kb = 204800  # 200MB = 204800KB
        write_offset_mb = 132
        generate_random_file(original_file, file_size_kb)

        # Шаг 9: Запись данных в /dev/nbd0 со смещением 132MB
        logger.info("=== Шаг 9: Запись данных в /dev/nbd0 со смещением 132MB ===")
        write_to_device(original_file, "/dev/nbd0", write_offset_mb)

        # Шаг 10: Чтение данных из /dev/nbd0 со смещением 132MB
        logger.info("=== Шаг 10: Чтение данных из /dev/nbd0 со смещением 132MB ===")
        read_file = "/tmp/read_data_200mb.bin"
        read_from_device("/dev/nbd0", read_file, write_offset_mb, file_size_kb)

        # Шаг 11: Сравнение исходного и прочитанного файлов
        logger.info("=== Шаг 11: Сравнение исходного и прочитанного файлов ===")
        if compare_files(original_file, read_file):
            logger.info("✅ Тест целостности данных пройден: файлы идентичны")
        else:
            logger.error("❌ Тест целостности данных не пройден: файлы различаются")
            raise RuntimeError("Тест целостности данных не пройден")

        # Шаг 12: Запуск FIO теста
        logger.info("=== Шаг 12: Запуск FIO теста ===")
        fio_command = [
            "sudo",
            "/home/vazhenin-mv/fio",
            "--name=randwrite",
            "--ioengine=libaio",
            "--iodepth=32",
            "--rw=randwrite",
            "--bs=4k",
            "--direct=1",
            "--numjobs=1",
            "--rate_iops=,32000",
            "--group_reporting",
            "--filename=/dev/nbd0"
        ]
        returncode, stdout, stderr = run_command(fio_command, check=True)
        logger.info("FIO тест успешно выполнен")

        logger.info("=== Все шаги успешно выполнены ===")

        # Если указан параметр --wait, переходим в режим ожидания
        if args.wait:
            logger.info("Режим ожидания: процессы будут продолжать работать до получения сигнала Ctrl-C")
            logger.info("Нажмите Ctrl-C для завершения всех процессов и выхода")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Получен сигнал прерывания, завершение работы...")

    except Exception as e:
        logger.error(f"Ошибка при выполнении скрипта: {e}")

        # Если указан параметр --wait, даже при ошибке переходим в режим ожидания
        if args.wait:
            logger.error("Режим ожидания: несмотря на ошибку процессы будут продолжать работать до получения сигнала Ctrl-C")
            logger.info("Нажмите Ctrl-C для завершения всех процессов и выхода")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Получен сигнал прерывания, завершение работы...")
    finally:
        # Удаляем временные файлы
        cleanup_temp_files("/tmp/random_data_200mb.bin", "/tmp/read_data_200mb.bin")

        # Завершаем все фоновые процессы
        process_manager.terminate_all()

def signal_handler(signum, frame):
    """Обработчик сигналов для корректного завершения"""
    logger.info(f"Получен сигнал {signum}, завершение работы...")
    sys.exit(0)

if __name__ == "__main__":
    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    main()
