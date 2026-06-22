# project-02-ai
<img width="1440" height="3640" alt="image" src="https://github.com/user-attachments/assets/1badc3d7-d6e8-465e-8b85-294aef256d1c" />


## 실행 방법
### 0. 패키지 설치
```bash
pip install -r requirements.txt
```

### 1. vllm 실행
```bash
nohup ./vllm.sh > vllm.log 2>&1 &
```
### 2. python 실행
```bash
python run_pipeline.py
```

## 필요 data & API
- [전국표준노드링크](https://www.its.go.kr/nodelink/nodelinkRef)
- [카카오 키워드로 장소 검색 API](https://developers.kakao.com/docs/ko/local/dev-guide#search-by-keyword)
- [카카오 주소로 좌표 검색 API](https://developers.kakao.com/docs/ko/local/dev-guide#address-coord)


## Requirements.txt 생성
``` bash
pip list --format=freeze > requirements.txt
```

## 커맨드
```bash
watch -n 1 free -h
watch -n 0.5 nvidia-smi
```
