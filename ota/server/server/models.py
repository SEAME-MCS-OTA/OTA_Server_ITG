"""
OTA Server - Database Models
Flask-SQLAlchemy ORM 모델 정의
"""
from datetime import datetime
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class Vehicle(db.Model):
    """차량 정보 모델"""
    __tablename__ = 'vehicles'

    # 기본 필드
    id = db.Column(db.Integer, primary_key=True)
    vehicle_id = db.Column(db.String(100), unique=True, nullable=False, index=True)
    current_version = db.Column(db.String(50))
    last_ip = db.Column(db.String(64))
    last_seen = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    status = db.Column(db.String(50), default='idle', index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # # 차량 메타데이터 (Phase 1)
    # name = db.Column(db.String(255))
    # model = db.Column(db.String(100))
    
    # # A/B 파티션 지원 (Phase 1)
    # partition_a_version = db.Column(db.String(50))
    # partition_b_version = db.Column(db.String(50))
    # active_partition = db.Column(db.String(1))  # 'A' or 'B'

    # Relationship
    update_histories = db.relationship('UpdateHistory', back_populates='vehicle')

    def to_dict(self):
        """딕셔너리로 변환"""
        return {
            'id': self.id,
            'vehicle_id': self.vehicle_id,
            'current_version': self.current_version,
            'last_ip': self.last_ip,
            'last_seen': self.last_seen.isoformat() if self.last_seen else None,
            'status': self.status,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            # 'name': self.name,
            # 'model': self.model,
            # 'partition_a_version': self.partition_a_version,
            # 'partition_b_version': self.partition_b_version,
            # 'active_partition': self.active_partition
        }
    
class Firmware(db.Model):
    """펌웨어 정보 모델"""
    __tablename__ = 'firmware'

    id = db.Column(db.Integer, primary_key=True)
    version = db.Column(db.String(50), unique=True, nullable=False, index=True)
    filename = db.Column(db.String(255), nullable=False)
    sha256 = db.Column(db.String(64), nullable=False)  # 명확한 해시 알고리즘 명시
    file_size = db.Column(db.BigInteger, nullable=False)
    file_path = db.Column(db.String(512), nullable=False)  # 전체 경로
    release_notes = db.Column(db.Text)
    is_active = db.Column(db.Boolean, default=True, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # # Phase 1: 업데이트 유형 및 설명 필드 추가
    # update_type = db.Column(db.String(20), default='full')  # 'full' or 'delta'
    # description = db.Column(db.Text)
    
    # Relationship
    update_histories = db.relationship('UpdateHistory', back_populates='firmware')

    def to_dict(self):
        """딕셔너리로 변환"""
        return {
            'id': self.id,
            'version': self.version,
            'filename': self.filename,
            'file_path': self.file_path,
            'file_size': self.file_size,
            'sha256': self.sha256,
            # 'update_type': self.update_type,
            'release_notes': self.release_notes,
            # 'description': self.description,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class UpdateHistory(db.Model):
    """업데이트 히스토리 모델"""
    __tablename__ = 'update_history'

    id = db.Column(db.Integer, primary_key=True)
    
    # 차량 및 펌웨어 참조
    vehicle_id = db.Column(db.String(100), db.ForeignKey('vehicles.vehicle_id'))
    firmware_id = db.Column(db.Integer, db.ForeignKey('firmware.id'))
    
    # 버전 정보
    from_version = db.Column(db.String(50))
    target_version = db.Column(db.String(50), nullable=False)
    
    # 업데이트 정보
    update_type = db.Column(db.String(20))  # 'full' or 'delta'
    
    # 상태 및 진행률
    status = db.Column(db.String(50), nullable=False, index=True)
    # downloading, verifying, installing, completed, failed, pending, rolled_back
    progress = db.Column(db.Integer, default=0)
    
    # 메시지 및 오류
    message = db.Column(db.Text)  # 범용 메시지
    client_log_json = db.Column(db.Text)
    client_log_path = db.Column(db.String(1024))
    client_log_updated_at = db.Column(db.DateTime)
    
    # 타임스탬프
    started_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    completed_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # # Phase 1: 롤백 관련 필드
    # error_message = db.Column(db.Text)  # Phase 1: 구조화된 에러
    # rollback_reason = db.Column(db.Text)  # Phase 1: 롤백 이유

    # Relationship
    vehicle = db.relationship('Vehicle', back_populates='update_histories')
    firmware = db.relationship('Firmware', back_populates='update_histories')

    def to_dict(self):
        """딕셔너리로 변환"""
        return {
            'id': self.id,
            'vehicle_id': self.vehicle_id,
            'firmware_id': self.firmware_id,
            'from_version': self.from_version,
            'target_version': self.target_version,
            'update_type': self.update_type,
            'status': self.status,
            'progress': self.progress,
            'message': self.message,
            'client_log_path': self.client_log_path,
            'client_log_updated_at': self.client_log_updated_at.isoformat() if self.client_log_updated_at else None,
            # 'error_message': self.error_message,
            # 'rollback_reason': self.rollback_reason,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
