# MSD-LMD Data Merging Tool

Công cụ merge dữ liệu MSD và LMD dựa trên vị trí không gian, làn đường, thời gian và chainage.

## Cấu trúc Project

```
DataAnalys/
├── __init__.py              # Package initialization
├── config.py                # ⚙️ Hằng số & cấu hình
├── file_utils.py            # 📁 Utilities cho file & input
├── data_preparation.py      # 🔧 Chuẩn bị dữ liệu
├── matching.py              # 🎯 Thuật toán matching
├── output.py                # 💾 Tạo & lưu kết quả
├── main.py                  # 🚀 Chương trình chính
├── gui.py                   # 🖥️ Giao diện PyQt6
├── README.md                # 📖 Tài liệu hướng dẫn
└── Merge_LMD-MSD_RPP.py     # 📄 File gốc (có thể xóa sau khi test)
```

## Cách sử dụng

### GUI Mode (Khuyến nghị)
```bash
python main.py
# hoặc
python gui.py
```
GUI cung cấp giao diện đồ họa thân thiện với:
- Chọn file bằng dialog
- Preview dữ liệu trước khi merge
- Cấu hình tham số dễ dàng
- Theo dõi tiến trình real-time
- Log chi tiết

### Console Mode
```bash
python main.py --console
```
Chế độ console với prompts tương tác.

### Sử dụng như module:
```python
from main import merge_msd_lmd
from data_preparation import prepare_msd_data
from matching import perform_spatial_matching
```

## Tính năng GUI

### 🖥️ Giao diện chính
- **File Selection**: Chọn MSD, LMD và thư mục output
- **Parameter Configuration**: Điều chỉnh tham số matching
- **Column Selection**: Chọn cột LMD để include
- **Data Preview**: Xem trước dữ liệu MSD/LMD
- **Progress Tracking**: Theo dõi tiến trình với progress bar
- **Real-time Logging**: Log chi tiết quá trình xử lý

### 🎛️ Điều khiển
- **Run Button**: Bắt đầu quá trình merge
- **Stop Button**: Dừng quá trình đang chạy
- **Column Management**: Chọn/xóa cột LMD
- **Preview Controls**: Load và xem dữ liệu

### 📊 Preview Data
- Hiển thị 100 dòng đầu của file
- Chuyển đổi giữa MSD và LMD
- Resize cột tự động

## Các Module

### config.py
Chứa tất cả hằng số và cấu hình:
- Danh sách cột LMD mặc định
- Tham số matching mặc định
- Cấu hình logging

### file_utils.py
- `select_file()`: Chọn file qua dialog
- `select_output_directory()`: Chọn thư mục output
- `get_user_input()`: Nhận input từ người dùng
- `select_lmd_columns()`: Chọn cột LMD

### data_preparation.py
- `prepare_msd_data()`: Chuẩn bị dữ liệu MSD
- `prepare_lmd_data()`: Chuẩn bị dữ liệu LMD

### matching.py
- `perform_spatial_matching()`: Matching không gian
- `filter_and_select_matches()`: Lọc và chọn best matches

### output.py
- `create_output_dataframe()`: Tạo dataframe kết quả
- `save_output()`: Lưu kết quả ra file CSV

### gui.py
- `MSDLMDMergerGUI`: Lớp GUI chính
- `WorkerThread`: Thread xử lý background
- `run_gui()`: Hàm chạy GUI

### main.py
Chương trình chính với logic chọn GUI/console.

## Dependencies

```
polars>=0.20.0
PyQt6>=6.0.0
scikit-learn>=1.0.0
numpy>=1.20.0
```

## Lợi ích của cấu trúc module

1. **Tái sử dụng**: Mỗi module có thể dùng riêng lẻ
2. **Dễ bảo trì**: Code được tổ chức theo chức năng
3. **Mở rộng**: Dễ dàng thêm module mới
4. **Test**: Có thể test từng phần riêng biệt
5. **GUI/Console**: Hỗ trợ cả hai chế độ

## Phát triển thêm

Để thêm module mới:
1. Tạo file `new_module.py`
2. Import vào `main.py` nếu cần
3. Cập nhật `config.py` cho hằng số mới
4. Cập nhật `README.md`

## Troubleshooting

### GUI không khởi động
```bash
pip install PyQt6
```

### Console mode nếu GUI không khả dụng
```bash
python main.py --console
```

### file_utils.py
- `select_file()`: Chọn file qua dialog
- `select_output_directory()`: Chọn thư mục output
- `get_user_input()`: Nhận input từ người dùng
- `select_lmd_columns()`: Chọn cột LMD

### data_preparation.py
- `prepare_msd_data()`: Chuẩn bị dữ liệu MSD
- `prepare_lmd_data()`: Chuẩn bị dữ liệu LMD

### matching.py
- `perform_spatial_matching()`: Matching không gian
- `filter_and_select_matches()`: Lọc và chọn best matches

### output.py
- `create_output_dataframe()`: Tạo dataframe kết quả
- `save_output()`: Lưu kết quả ra file CSV

### main.py
Chương trình chính với giao diện người dùng.

## Lợi ích của cấu trúc module

1. **Tái sử dụng**: Mỗi module có thể dùng riêng lẻ
2. **Dễ bảo trì**: Code được tổ chức theo chức năng
3. **Mở rộng**: Dễ dàng thêm module mới
4. **Test**: Có thể test từng phần riêng biệt
5. **Độc lập**: Các module ít phụ thuộc lẫn nhau

## Phát triển thêm

Để thêm module mới:
1. Tạo file `new_module.py`
2. Import vào `main.py` nếu cần
3. Cập nhật `config.py` nếu có hằng số mới
4. Cập nhật README.md