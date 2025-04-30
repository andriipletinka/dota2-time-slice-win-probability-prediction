import torch
import torch.nn as nn
import torch.nn.functional as F


class TimesliceTransformer(nn.Module):
    def __init__(
        self,
        hero_vocab_size: int,
        item_vocab_size: int,
        max_seq_len: int,
        hero_emb_dim: int = 16,
        item_emb_dim: int = 8,
        num_player_numeric: int = 19,
        num_team_numeric: int = 31,
        model_dim: int = 512,
        nhead: int = 16,
        ff_dim: int = 1024,
        num_layers: int = 8,
        dropout: float = 0.2
    ):
        super().__init__()

        self.hero_emb = nn.Embedding(hero_vocab_size, hero_emb_dim)
        self.item_emb = nn.Embedding(item_vocab_size, item_emb_dim, padding_idx=0)

        player_cat_dim = hero_emb_dim + 6 * item_emb_dim
        total_player_dim = player_cat_dim + num_player_numeric
        team_dim = 2 * num_team_numeric
        self.input_dim = 10 * total_player_dim + team_dim

        self.input_proj = nn.Linear(self.input_dim, model_dim)
        self.input_norm = nn.LayerNorm(model_dim)
        self.dropout = nn.Dropout(dropout)

        pos_enc = torch.zeros(max_seq_len, model_dim)
        position = torch.arange(max_seq_len).unsqueeze(1).float()
        div_term = torch.exp(torch.arange(0, model_dim, 2).float() * -(torch.log(torch.tensor(10000.0)) / model_dim))
        pos_enc[:, 0::2] = torch.sin(position * div_term)
        pos_enc[:, 1::2] = torch.cos(position * div_term)
        self.register_buffer('positional_encoding', pos_enc.unsqueeze(0))

        encoder_layer = nn.TransformerEncoderLayer(
            d_model=model_dim,
            nhead=nhead,
            dim_feedforward=ff_dim,
            dropout=dropout,
            activation='gelu',
            batch_first=True,
        )
        self.transformer_encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)

        decoder_layer = nn.TransformerDecoderLayer(
            d_model=model_dim,
            nhead=nhead,
            dim_feedforward=ff_dim,
            dropout=dropout,
            activation='gelu',
            batch_first=True,
        )
        self.transformer_decoder = nn.TransformerDecoder(decoder_layer, num_layers=num_layers)

        self.sos_token = nn.Parameter(torch.zeros(1, 1, model_dim))

        out_dim = 10 * num_player_numeric + 2 * num_team_numeric
        self.pred_head = nn.Linear(model_dim, out_dim)

    def forward(self, hero_ids, item_ids, player_nums, team_nums):
        B, S, _, _ = item_ids.shape

        ts_vec = self._build_input_vectors(hero_ids, item_ids, player_nums, team_nums)
        x = self.input_proj(ts_vec)

        x = x + self.positional_encoding[:, :S, :]
        x = self.input_norm(x)
        x = self.dropout(x)

        memory = self.transformer_encoder(x)

        tgt = self.sos_token.expand(B, -1, -1)
        tgt = tgt + self.positional_encoding[:, :1, :]

        dec = self.transformer_decoder(tgt, memory)

        last = dec[:, -1, :] 
        pred_next = self.pred_head(last)
        return pred_next

    def encode_sequence(self, hero_ids, item_ids, player_nums, team_nums):
        with torch.no_grad():
            ts_vec = self._build_input_vectors(hero_ids, item_ids, player_nums, team_nums)
            x = self.input_proj(ts_vec)
            S = hero_ids.size(1)
            x = x + self.positional_encoding[:, :S, :]
            x = self.input_norm(x)
            memory = self.transformer_encoder(x)
            return memory[:, -1, :]

    def _build_input_vectors(self, hero_ids, item_ids, player_nums, team_nums):
        B, S, P, _ = item_ids.shape

        h_emb = self.hero_emb(hero_ids)
        i_emb = self.item_emb(item_ids)

        i_flat = i_emb.view(B, S, 10, -1)

        p_feat = torch.cat([h_emb, i_flat, player_nums], dim=-1)
        p_flat = p_feat.view(B, S, -1)

        t_flat = team_nums.view(B, S, -1)

        return torch.cat([p_flat, t_flat], dim=-1)
    

class OutcomeMLP(nn.Module):
    def __init__(self, model_dim: int, hidden_dim: int = 216, dropout: float = 0.1):
        super().__init__()
        self.fc1 = nn.Linear(model_dim, hidden_dim)
        self.act = nn.GELU()
        self.dropout = nn.Dropout(dropout)
        self.fc2 = nn.Linear(hidden_dim, 1)

    def forward(self, x):
        x = self.fc1(x)
        x = self.act(x)
        x = self.dropout(x)
        return self.fc2(x).squeeze(-1)
