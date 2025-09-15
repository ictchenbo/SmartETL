import os
import json


class IncrementalStringMapper:
    def __init__(self, save_path: str, initial_strings: set = None):
        """
        :param save_path JSON 文件路径，用于从磁盘加载已有映射
        :param initial_strings: 可选的初始字符串列表（用于预加载）
        """
        self.str_to_int = {}  # 字符串 -> 整数
        self.int_to_str = {}  # 整数 -> 字符串
        self.next_id = 0      # 下一个可用整数 ID
        self.save_path = save_path

        if os.path.exists(save_path):
            self.load()

        if initial_strings:
            for s in initial_strings:
                self.get_id(s)  # 自动注册

    def get_id(self, s: str) -> int:
        """获取或分配字符串对应的整数 ID"""
        if s not in self.str_to_int:
            self.str_to_int[s] = self.next_id
            self.int_to_str[self.next_id] = s
            self.next_id += 1
            self.save()
        return self.str_to_int[s]

    def __call__(self, s: str, *args, **kwargs):
        return self.get_id(s)

    def get_string(self, i: int) -> str:
        """根据整数 ID 获取字符串"""
        return self.int_to_str.get(i)

    def __len__(self):
        return len(self.str_to_int)

    def save(self):
        """保存映射到 JSON 文件（便于重启后恢复）"""
        data = {
            "str_to_int": self.str_to_int,
            "next_id": self.next_id
        }
        with open(self.save_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"✅ 映射已保存到 {self.save_path}")

    def load(self):
        """从 JSON 文件加载映射"""
        with open(self.save_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        self.str_to_int = data["str_to_int"]
        self.next_id = data["next_id"]
        # 重建 int_to_str 反向映射
        self.int_to_str = {i: s for s, i in self.str_to_int.items()}
        print(f"✅ 映射已从 {self.save_path} 加载，共 {len(self)} 个字符串")

    def __repr__(self):
        return f"IncrementalStringMapper(size={len(self)}, next_id={self.next_id})"
