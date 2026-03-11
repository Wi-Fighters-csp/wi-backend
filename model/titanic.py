from copy import deepcopy
from threading import Lock

import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.tree import DecisionTreeClassifier


class TitanicModel:
    """Train once and reuse a Titanic survival prediction model."""

    _instance = None
    _lock = Lock()
    REQUIRED_FIELDS = (
        'pclass',
        'sex',
        'age',
        'sibsp',
        'parch',
        'fare',
        'embarked',
        'alone',
    )

    def __init__(self):
        self.model = None
        self.dt = None
        self.accuracy = None
        self.encoder = OneHotEncoder(handle_unknown='ignore')
        self.base_features = ['pclass', 'sex', 'age', 'sibsp', 'parch', 'fare', 'alone']
        self.features = list(self.base_features)
        self.target = 'survived'
        self.embarked_columns = []
        self.embarked_survival_rates = {}
        self.reference_values = {}
        self.training_rows = 0
        self.test_rows = 0
        self.titanic_data = self._load_dataset()

    def _load_dataset(self):
        try:
            return sns.load_dataset('titanic')
        except Exception as exc:
            raise RuntimeError(
                'Unable to load the Titanic dataset. Ensure seaborn can access its dataset cache.'
            ) from exc

    def _clean(self):
        data = self.titanic_data.copy()
        data = data.drop(
            ['alive', 'who', 'adult_male', 'class', 'embark_town', 'deck'],
            axis=1,
            errors='ignore',
        )

        data['sex'] = data['sex'].map({'male': 1, 'female': 0})
        data['alone'] = data['alone'].astype(int)
        data = data.dropna(subset=['embarked'])

        self.embarked_survival_rates = (
            data.groupby('embarked')[self.target].mean().sort_values(ascending=False).to_dict()
        )

        onehot = self.encoder.fit_transform(data[['embarked']]).toarray()
        self.embarked_columns = [f'embarked_{value}' for value in self.encoder.categories_[0]]
        onehot_df = pd.DataFrame(onehot, columns=self.embarked_columns, index=data.index)
        data = pd.concat([data.drop(['embarked'], axis=1), onehot_df], axis=1)

        data = data.dropna(subset=self.base_features + [self.target])
        self.features = self.base_features + self.embarked_columns
        self.titanic_data = data
        self.reference_values = {
            'fare_by_pclass': data.groupby('pclass')['fare'].median().to_dict(),
            'best_embarked': next(iter(self.embarked_survival_rates), 'C'),
        }

    def _train(self):
        x_data = self.titanic_data[self.features]
        y_data = self.titanic_data[self.target]

        x_train, x_test, y_train, y_test = train_test_split(
            x_data,
            y_data,
            test_size=0.2,
            random_state=42,
            stratify=y_data,
        )

        self.model = LogisticRegression(max_iter=1000, random_state=42)
        self.model.fit(x_train, y_train)

        self.dt = DecisionTreeClassifier(max_depth=4, random_state=42)
        self.dt.fit(x_train, y_train)

        self.accuracy = float(accuracy_score(y_test, self.model.predict(x_test)))
        self.training_rows = int(len(x_train))
        self.test_rows = int(len(x_test))

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
                    cls._instance._clean()
                    cls._instance._train()
        return cls._instance

    def _unwrap_value(self, value):
        if isinstance(value, list):
            if len(value) != 1:
                raise ValueError('Each Titanic passenger field must contain a single value.')
            return value[0]
        return value

    def _normalize_bool(self, value):
        value = self._unwrap_value(value)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {'true', '1', 'yes', 'y', 'on'}:
                return True
            if lowered in {'false', '0', 'no', 'n', 'off'}:
                return False
        return bool(value)

    def _normalize_passenger(self, passenger):
        if not isinstance(passenger, dict):
            raise ValueError('Passenger data must be a JSON object.')

        missing = [field for field in self.REQUIRED_FIELDS if field not in passenger]
        if missing:
            raise ValueError(f"Missing required passenger fields: {', '.join(missing)}")

        normalized = {
            'pclass': int(self._unwrap_value(passenger['pclass'])),
            'sex': str(self._unwrap_value(passenger['sex'])).strip().lower(),
            'age': float(self._unwrap_value(passenger['age'])),
            'sibsp': int(self._unwrap_value(passenger['sibsp'])),
            'parch': int(self._unwrap_value(passenger['parch'])),
            'fare': float(self._unwrap_value(passenger['fare'])),
            'embarked': str(self._unwrap_value(passenger['embarked'])).strip().upper(),
            'alone': self._normalize_bool(passenger['alone']),
        }

        if normalized['pclass'] not in {1, 2, 3}:
            raise ValueError('pclass must be 1, 2, or 3.')
        if normalized['sex'] not in {'male', 'female'}:
            raise ValueError("sex must be 'male' or 'female'.")
        if normalized['embarked'] not in set(self.encoder.categories_[0]):
            valid_ports = ', '.join(self.encoder.categories_[0])
            raise ValueError(f'embarked must be one of: {valid_ports}')
        if normalized['age'] <= 0:
            raise ValueError('age must be greater than 0.')
        if normalized['sibsp'] < 0 or normalized['parch'] < 0:
            raise ValueError('sibsp and parch cannot be negative.')
        if normalized['fare'] < 0:
            raise ValueError('fare cannot be negative.')

        return normalized

    def _passenger_frame(self, passenger):
        normalized = self._normalize_passenger(passenger)
        passenger_df = pd.DataFrame([normalized])
        passenger_df['sex'] = passenger_df['sex'].map({'male': 1, 'female': 0})
        passenger_df['alone'] = passenger_df['alone'].astype(int)

        onehot = self.encoder.transform(passenger_df[['embarked']]).toarray()
        onehot_df = pd.DataFrame(onehot, columns=self.embarked_columns, index=passenger_df.index)
        passenger_df = pd.concat([passenger_df.drop(['embarked'], axis=1), onehot_df], axis=1)
        passenger_df = passenger_df.reindex(columns=self.features, fill_value=0)
        return normalized, passenger_df

    def _predict_probability(self, passenger):
        _, passenger_df = self._passenger_frame(passenger)
        die, survive = np.squeeze(self.model.predict_proba(passenger_df))
        return float(die), float(survive), passenger_df

    def _format_feature_name(self, feature):
        if feature.startswith('embarked_'):
            return f"Embarked from {feature.split('_', 1)[1]}"
        labels = {
            'pclass': 'Passenger class',
            'sex': 'Sex',
            'age': 'Age',
            'sibsp': 'Siblings or spouse aboard',
            'parch': 'Parents or children aboard',
            'fare': 'Fare',
            'alone': 'Traveling alone',
        }
        return labels.get(feature, feature)

    def _top_factors(self, passenger_df):
        coefficients = np.squeeze(self.model.coef_)
        contributions = coefficients * passenger_df.iloc[0].to_numpy(dtype=float)
        factors = []

        for feature, contribution, value in zip(self.features, contributions, passenger_df.iloc[0].to_list()):
            if abs(contribution) < 1e-9:
                continue
            factors.append(
                {
                    'feature': feature,
                    'label': self._format_feature_name(feature),
                    'impact': float(contribution),
                    'direction': 'helps' if contribution > 0 else 'hurts',
                    'value': float(value),
                }
            )

        factors.sort(key=lambda item: abs(item['impact']), reverse=True)
        return factors[:5]

    def _candidate_first_class(self, passenger):
        if passenger['pclass'] == 1:
            return None
        candidate = deepcopy(passenger)
        candidate['pclass'] = 1
        candidate['fare'] = float(max(candidate['fare'], self.reference_values['fare_by_pclass'].get(1, candidate['fare'])))
        return candidate, 'Book first class with a fare closer to typical first-class tickets.'

    def _candidate_companion(self, passenger):
        if not passenger['alone'] and passenger['sibsp'] > 0:
            return None
        candidate = deepcopy(passenger)
        candidate['alone'] = False
        candidate['sibsp'] = max(1, candidate['sibsp'])
        return candidate, 'Travel with at least one close companion instead of boarding alone.'

    def _candidate_embarked(self, passenger):
        best_port = self.reference_values.get('best_embarked')
        if not best_port or passenger['embarked'] == best_port:
            return None
        candidate = deepcopy(passenger)
        candidate['embarked'] = best_port
        return candidate, f'Embark from {best_port}, which had the strongest survival rate in the training data.'

    def _build_improvement_plan(self, passenger, baseline_survival):
        current = deepcopy(passenger)
        current_survival = baseline_survival
        changes = []

        for builder in (self._candidate_first_class, self._candidate_companion, self._candidate_embarked):
            proposal = builder(current)
            if proposal is None:
                continue

            candidate, reason = proposal
            _, candidate_survival, _ = self._predict_probability(candidate)
            improvement = candidate_survival - current_survival
            if improvement <= 0.005:
                continue

            diffs = []
            for field in self.REQUIRED_FIELDS:
                if current[field] != candidate[field]:
                    diffs.append(
                        {
                            'field': field,
                            'from': current[field],
                            'to': candidate[field],
                        }
                    )

            changes.append(
                {
                    'reason': reason,
                    'delta': float(improvement),
                    'changes': diffs,
                }
            )
            current = candidate
            current_survival = candidate_survival

        return {
            'survive': float(current_survival),
            'delta': float(current_survival - baseline_survival),
            'changes': changes,
            'passenger': current,
        }

    def predict(self, passenger):
        """Predict survival using passenger keys: pclass, sex, age, sibsp, parch, fare, embarked, alone."""
        normalized, passenger_df = self._passenger_frame(passenger)
        die, survive = np.squeeze(self.model.predict_proba(passenger_df))

        return {
            'die': float(die),
            'survive': float(survive),
            'accuracy': float(self.accuracy or 0.0),
            'top_factors': self._top_factors(passenger_df),
            'improvement': self._build_improvement_plan(normalized, float(survive)),
        }

    def feature_weights(self):
        importances = self.dt.feature_importances_
        weighted = {
            feature: float(importance)
            for feature, importance in zip(self.features, importances)
        }
        return dict(sorted(weighted.items(), key=lambda item: item[1], reverse=True))

    def metadata(self):
        return {
            'accuracy': float(self.accuracy or 0.0),
            'training_rows': self.training_rows,
            'test_rows': self.test_rows,
            'features': list(self.features),
            'feature_weights': self.feature_weights(),
            'best_embarked': self.reference_values.get('best_embarked'),
        }


def initTitanic():
    TitanicModel.get_instance()


def testTitanic():
    passenger = {
        'pclass': 2,
        'sex': 'male',
        'age': 65,
        'sibsp': 1,
        'parch': 1,
        'fare': 16.00,
        'embarked': 'S',
        'alone': False,
    }

    titanic_model = TitanicModel.get_instance()
    probability = titanic_model.predict(passenger)
    print('death probability: {:.2%}'.format(probability.get('die')))
    print('survival probability: {:.2%}'.format(probability.get('survive')))

    for feature, importance in titanic_model.feature_weights().items():
        print(feature, f'{importance:.2%}')


if __name__ == '__main__':
    testTitanic()